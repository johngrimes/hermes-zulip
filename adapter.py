"""Zulip gateway adapter for Hermes Agent.

Connects to a Zulip organisation via the v1 REST API and the event-queue
long-polling stream. No external Zulip library is required - everything is
driven through ``aiohttp`` (already a Hermes dependency), mirroring the
Mattermost adapter's transport strategy.

Conversations are keyed by chat IDs (JIDs) of the following form, so each
stream topic and each direct-message partner gets its own session:

* Stream topic  -> ``zulip:{stream_id}:{topic}``
* Direct message -> ``zulip:dm:{user_id}``

Environment variables:

    ZULIP_SITE               Organisation URL (e.g. https://org.zulipchat.com)
    ZULIP_BOT_EMAIL          Bot account email
    ZULIP_BOT_API_KEY        Bot API key
    ZULIP_STREAM             Stream the bot operates on (name). When set,
                             messages from other streams are ignored.
    ZULIP_ALLOWED_USERS      Comma-separated Zulip user IDs allowed to talk
    ZULIP_ALLOW_ALL_USERS    Allow any Zulip user (dev only)
    ZULIP_HOME_CHANNEL       Home JID for cron / notification delivery
    ZULIP_REQUIRE_MENTION    Require @mention in streams (default true)
    ZULIP_FREE_RESPONSE_TOPICS  Topics where @mention is not required

Author: John Grimes
"""

from __future__ import annotations

import asyncio
import base64
import html as html_lib
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from gateway.config import Platform, PlatformConfig
from gateway.platforms.helpers import MessageDeduplicator
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)
# Profile-scoped env reads: under the multiplexing gateway a secondary profile's
# .env lives in a per-task secret scope, NOT os.environ (which holds the default
# profile's values). Reading os.getenv would make every profile log in as the
# default profile's bot.
from gateway.platforms._shared import (
    apply_yaml_bridge,
    env_is_connected,
    extra_or_secret,
    get_scoped_secret,
)

logger = logging.getLogger(__name__)

# Zulip's hard per-message limit is 10000 characters. Chunk above it so
# long agent responses are delivered in full rather than rejected.
MAX_MESSAGE_LENGTH = 10000

# Reconnect backoff for the event-polling loop.
_RECONNECT_BASE_DELAY = 2.0
_RECONNECT_MAX_DELAY = 60.0
_RECONNECT_JITTER = 0.2

# Regex matching anchor tags whose href points at a Zulip user upload. Zulip
# renders attachment links as <a href="/user_uploads/...">filename</a>; the
# raw upload path requires Basic auth to download, so the adapter fetches
# each one and caches it locally for downstream tools.
_UPLOAD_LINK_RE = re.compile(
    r'<a\s+[^>]*href="(?:https?://[^/]+)?(/user_uploads/[^"]+)"[^>]*>(.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)


def check_zulip_requirements() -> bool:
    """Return True if the Zulip adapter has what it needs to run."""
    if not get_scoped_secret("ZULIP_BOT_API_KEY", "", external_fallback=True):
        logger.debug("Zulip: ZULIP_BOT_API_KEY not set")
        return False
    if not get_scoped_secret("ZULIP_SITE", "", external_fallback=True):
        logger.warning("Zulip: ZULIP_SITE not set")
        return False
    if not get_scoped_secret("ZULIP_BOT_EMAIL", "", external_fallback=True):
        logger.warning("Zulip: ZULIP_BOT_EMAIL not set")
        return False
    try:
        import aiohttp  # noqa: F401

        return True
    except ImportError:
        logger.warning("Zulip: aiohttp not installed")
        return False


def parse_jid(jid: str) -> Optional[Dict[str, Any]]:
    """Parse a Zulip JID into its routing components.

    @param jid - The JID string ("zulip:{stream_id}:{topic}" or "zulip:dm:{user_id}").
    @returns A dict describing the target, or None if the JID is not a valid Zulip JID.
    """
    if not jid.startswith("zulip:"):
        return None
    rest = jid[len("zulip:"):]

    if rest.startswith("dm:"):
        user_id = parseIntSafe(rest[len("dm:"):])
        if user_id is None:
            return None
        return {"type": "dm", "user_id": user_id}

    colon_idx = rest.find(":")
    if colon_idx == -1:
        return None
    stream_id = parseIntSafe(rest[:colon_idx])
    if stream_id is None:
        return None
    topic = rest[colon_idx + 1:]
    if not topic:
        return None
    return {"type": "stream", "stream_id": stream_id, "topic": topic}


def parseIntSafe(value: str) -> Optional[int]:
    """Parse an integer, returning None on failure instead of raising."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def build_send_params(jid: str, content: str) -> Optional[Dict[str, Any]]:
    """Build a Zulip POST /messages payload from a JID and message text.

    @param jid - The target JID.
    @param content - The message content (Zulip markdown).
    @returns The JSON payload for POST /api/v1/messages, or None for an invalid JID.
    """
    parsed = parse_jid(jid)
    if not parsed:
        return None
    if parsed["type"] == "stream":
        return {
            "type": "stream",
            "to": parsed["stream_id"],
            "topic": parsed["topic"],
            "content": content,
        }
    return {
        "type": "direct",
        "to": [parsed["user_id"]],
        "content": content,
    }


def html_to_text(html: str) -> str:
    """Convert Zulip's rendered HTML message content to plain text.

    Zulip delivers inbound message bodies as HTML. The agent works with
    plain text, so this collapses block boundaries into newlines, strips
    remaining tags, and decodes entities. It is intentionally lossy - the
    goal is readable text, not a faithful HTML round-trip.

    @param html - The rendered HTML content from a Zulip message event.
    @returns A plain-text approximation suitable for the agent.
    """
    if not html:
        return ""
    # Block-level boundaries become newlines so paragraphs and list items
    # stay on separate lines.
    text = re.sub(r"(?i)</(p|div|li|br|h[1-6])>", "\n", html)
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    # Drop everything else that is tag-shaped.
    text = re.sub(r"<[^>]+>", "", text)
    text = html_lib.unescape(text)
    # Collapse runs of spaces (but preserve newlines) and trim each line.
    text = re.sub(r"[^\S\n]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{2,}", "\n", text)
    return text.strip()


def extract_attachment_links(html: str) -> List[Tuple[str, str]]:
    """Find user-upload attachment links in rendered Zulip HTML.

    @param html - The rendered HTML content from a Zulip message event.
    @returns A list of (upload_path, filename) pairs for /user_uploads/ links.
    """
    results: List[Tuple[str, str]] = []
    for match in _UPLOAD_LINK_RE.finditer(html or ""):
        href = match.group(1)
        label = re.sub(r"<[^>]+>", "", match.group(2) or "").strip()
        results.append((href, label or Path(href).name or "attachment"))
    return results


# Opening <span> tags; Zulip renders user mentions as
# <span class="user-mention" data-user-id="...">@Name</span>.
_SPAN_TAG_RE = re.compile(r"<span\b[^>]*>", re.IGNORECASE)
_DATA_USER_ID_RE = re.compile(r'data-user-id="([^"]*)"', re.IGNORECASE)


def _at_mention_pattern(token: str) -> str:
    """Regex for "@token" as a whole mention (not inside a longer name)."""
    return r"(?<![\w@])@" + re.escape(token) + r"(?![\w-])"


def is_bot_mentioned(
    flags: Optional[List[str]],
    html: str,
    text: str,
    bot_user_id: Optional[int],
    mention_tokens: List[str],
) -> bool:
    """Decide whether a stream message mentions the bot.

    Trusts Zulip's own signals first: the ``mentioned`` message flag, then a
    rendered user-mention span carrying the bot's user ID. Falls back to an
    explicit ``@token`` in the text. A token is never matched without its
    ``@``, so ordinary words and numbers (such as the bot's user ID) in a
    message are not mistaken for a mention.

    @param flags - The event's message flags (e.g. ["read", "mentioned"]).
    @param html - The rendered HTML content of the message.
    @param text - The plain-text form of the message.
    @param bot_user_id - The bot's Zulip user ID, if known.
    @param mention_tokens - Strings the bot can be @-mentioned with.
    @returns True when the message mentions the bot.
    """
    if flags and "mentioned" in flags:
        return True
    if bot_user_id is not None:
        for tag in _SPAN_TAG_RE.finditer(html or ""):
            if "user-mention" not in tag.group(0):
                continue
            uid = _DATA_USER_ID_RE.search(tag.group(0))
            if uid and uid.group(1) == str(bot_user_id):
                return True
    return any(
        token and re.search(_at_mention_pattern(token), text or "", flags=re.IGNORECASE)
        for token in mention_tokens
    )


def strip_mention(text: str, mention_tokens: List[str]) -> str:
    """Remove @-mentions of the bot so the agent sees clean input.

    Only ``@token`` forms are removed, longest token first so a full name
    ("@Hermes Bot") wins over a shorter username ("@hermes"). Bare words and
    numbers are left alone.

    @param text - The raw message text.
    @param mention_tokens - Strings the platform uses to mention the bot (e.g. username, user id).
    @returns The text with the bot's mentions removed.
    """
    cleaned = text
    for token in sorted((t for t in mention_tokens if t), key=len, reverse=True):
        cleaned = re.sub(_at_mention_pattern(token), "", cleaned, flags=re.IGNORECASE)
    return re.sub(r"[^\S\n]{2,}", " ", cleaned).strip()


class ZulipAdapter(BasePlatformAdapter):
    """Gateway adapter for Zulip (cloud or self-hosted) via REST + event queue."""

    splits_long_messages = True  # send() chunks via truncate_message(MAX_MESSAGE_LENGTH)

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform("zulip"))

        extra = getattr(config, "extra", {}) or {}
        self._site: str = (
            extra.get("site", "") or get_scoped_secret("ZULIP_SITE", "")
        ).rstrip("/")
        self._email: str = (
            extra.get("bot_email", "") or get_scoped_secret("ZULIP_BOT_EMAIL", "")
        )
        self._api_key: str = config.token or get_scoped_secret("ZULIP_BOT_API_KEY", "")
        self._stream_name: str = (
            extra.get("stream", "") or get_scoped_secret("ZULIP_STREAM", "")
            or get_scoped_secret("ZULIP_CHANNEL", "")
        )

        self._bot_user_id: Optional[int] = None
        self._bot_full_name: str = ""

        self._anointed_stream_id: Optional[int] = None

        self._session: Any = None  # aiohttp.ClientSession
        self._poll_task: Optional[asyncio.Task] = None
        self._queue_id: Optional[str] = None
        self._last_event_id: int = -1
        self._closing = False

        self._dedup = MessageDeduplicator()

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _auth(self) -> str:
        # Zulip uses HTTP Basic auth with "email:api_key", base64-encoded.
        token = f"{self._email}:{self._api_key}"
        return "Basic " + base64.b64encode(token.encode("utf-8")).decode("ascii")

    def _headers(self) -> Dict[str, str]:
        # The message send endpoint takes form-urlencoded bodies; JSON is also
        # accepted. We use form encoding to match the public API examples and
        # avoid content-type negotiation edge cases.
        return {"Authorization": self._auth()}

    async def _api_post_form(
        self, path: str, data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """POST form data to /api/v1/{path} and return the parsed JSON."""
        return await self._api_form("POST", path, data)

    async def _api_patch_form(
        self, path: str, data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """PATCH form data to /api/v1/{path} and return the parsed JSON."""
        return await self._api_form("PATCH", path, data)

    async def _api_form(
        self, method: str, path: str, data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Send a form-encoded request to /api/v1/{path} and return parsed JSON."""
        import aiohttp

        url = f"{self._site}/api/v1/{path.lstrip('/')}"
        try:
            async with self._session.request(
                method,
                url,
                headers=self._headers(),
                data=data,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                body = await resp.json()
                if resp.status >= 400 or body.get("result") != "success":
                    logger.error(
                        "Zulip %s %s -> %s: %s",
                        method,
                        path,
                        resp.status,
                        body.get("msg", "")[:200],
                    )
                return body
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.error("Zulip %s %s network error: %s", method, path, exc)
            return {"result": "error", "msg": str(exc)}

    async def _api_get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """GET /api/v1/{path} and return the parsed JSON."""
        import aiohttp

        url = f"{self._site}/api/v1/{path.lstrip('/')}"
        try:
            async with self._session.get(
                url,
                headers=self._headers(),
                params=params,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                body = await resp.json()
                if resp.status >= 400 or body.get("result") != "success":
                    logger.error(
                        "Zulip GET %s -> %s: %s",
                        path,
                        resp.status,
                        body.get("msg", "")[:200],
                    )
                return body
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.error("Zulip GET %s network error: %s", path, exc)
            return {"result": "error", "msg": str(exc)}

    # ------------------------------------------------------------------
    # Required overrides
    # ------------------------------------------------------------------

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        """Authenticate, resolve the configured stream, and start polling."""
        import aiohttp

        if not self._site or not self._email or not self._api_key:
            logger.error("Zulip: site, bot email, or API key not configured")
            return False

        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30)
        )
        self._closing = False

        me = await self._api_get("users/me")
        if me.get("result") != "success":
            logger.error(
                "Zulip: failed to authenticate - check ZULIP_BOT_EMAIL and ZULIP_BOT_API_KEY"
            )
            await self._session.close()
            return False
        self._bot_user_id = me.get("user_id")
        self._bot_full_name = me.get("full_name", "")
        logger.info(
            "Zulip: authenticated as %s (user_id=%s) on %s",
            self._email,
            self._bot_user_id,
            self._site,
        )

        if self._stream_name:
            resolved = await self._api_get(
                "get_stream_id", {"stream": self._stream_name}
            )
            if resolved.get("result") == "success":
                self._anointed_stream_id = resolved.get("stream_id")
                logger.info(
                    "Zulip: operating on stream %s (id=%s)",
                    self._stream_name,
                    self._anointed_stream_id,
                )
            else:
                logger.warning(
                    "Zulip: could not resolve stream %r - bot will respond in all subscribed streams",
                    self._stream_name,
                )

        self._poll_task = asyncio.create_task(self._poll_loop())
        self._mark_connected()
        return True

    async def disconnect(self) -> None:
        """Stop polling and close the HTTP session."""
        self._closing = True
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            try:
                await self._poll_task
            except (asyncio.CancelledError, Exception):
                pass
        if self._session and not self._session.closed:
            await self._session.close()
        logger.info("Zulip: disconnected")

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a message (chunked if long) to a stream topic or DM."""
        if not content:
            return SendResult(success=True)

        params = build_send_params(chat_id, self.format_message(content))
        if not params:
            return SendResult(success=False, error=f"Invalid Zulip JID: {chat_id}")

        chunks = self.truncate_message(params["content"], MAX_MESSAGE_LENGTH)
        last_id: Optional[str] = None
        for chunk in chunks:
            payload = dict(params)
            payload["content"] = chunk
            # reply_to on Zulip is a message id that threads the reply in the
            # same topic. Only meaningful for stream messages.
            if reply_to:
                payload["reply_to"] = reply_to
            result = await self._api_post_form("messages", encode_form_params(payload))
            if result.get("result") != "success":
                return SendResult(success=False, error=result.get("msg", "send failed"))
            last_id = str(result.get("id", "")) if result.get("id") is not None else last_id
        return SendResult(success=True, message_id=last_id)

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Return a best-effort name and type for a JID."""
        parsed = parse_jid(chat_id)
        if not parsed:
            return {"name": chat_id, "type": "channel"}
        if parsed["type"] == "dm":
            info = await self._api_get(f"users/{parsed['user_id']}")
            name = info.get("user", {}).get("full_name", chat_id) if info.get("result") == "success" else chat_id
            return {"name": name, "type": "dm"}
        stream_name = await self._stream_name_for_id(parsed["stream_id"])
        return {
            "name": f"{stream_name} > {parsed['topic']}",
            "type": "group",
        }

    # ------------------------------------------------------------------
    # Optional overrides
    # ------------------------------------------------------------------

    async def send_typing(
        self, chat_id: str, metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Send a typing indicator for DMs. Zulip typing notifications are DM-only."""
        parsed = parse_jid(chat_id)
        if not parsed or parsed["type"] != "dm" or self._bot_user_id is None:
            return
        await self._api_post_form(
            "typing", encode_form_params({"to": [parsed["user_id"]], "op": "start"})
        )

    async def edit_message(
        self, chat_id: str, message_id: str, content: str, *, finalize: bool = False
    ) -> SendResult:
        """Edit a previously sent message. Zulip supports topic/content edits.

        Zulip's update-message endpoint is PATCH /api/v1/messages/{id}, not
        POST (see https://zulip.com/api/update-message). Using POST returns
        HTTP 405 and breaks streamed message edits.
        """
        result = await self._api_patch_form(
            f"messages/{message_id}",
            {"content": self.format_message(content)},
        )
        if result.get("result") != "success":
            return SendResult(success=False, error=result.get("msg", "edit failed"))
        return SendResult(success=True, message_id=message_id)

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Download an image and upload it as a Zulip attachment."""
        return await self._send_url_as_file(chat_id, image_url, caption, metadata)

    async def send_image_file(
        self,
        chat_id: str,
        image_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Upload a local image file."""
        return await self._send_local_file(chat_id, image_path, caption, metadata)

    async def send_document(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Upload a local file as a document."""
        return await self._send_local_file(chat_id, file_path, caption, metadata, file_name)

    def format_message(self, content: str) -> str:
        """Zulip renders markdown. Strip image markdown to bare links."""
        return re.sub(r"!\[([^\]]*)\]\(([^)]+)\)", r"\2", content)

    # ------------------------------------------------------------------
    # File helpers
    # ------------------------------------------------------------------

    async def _upload_file(
        self, file_data: bytes, filename: str, content_type: str
    ) -> Optional[str]:
        """Upload a file to Zulip and return its user_uploads URL."""
        import aiohttp

        form = aiohttp.FormData()
        form.add_field("file", file_data, filename=filename, content_type=content_type)
        async with self._session.post(
            f"{self._site}/api/v1/user_uploads",
            headers=self._headers(),
            data=form,
            timeout=aiohttp.ClientTimeout(total=60),
        ) as resp:
            body = await resp.json()
            if resp.status >= 400 or body.get("result") != "success":
                logger.error("Zulip file upload -> %s: %s", resp.status, body.get("msg", "")[:200])
                return None
            return body.get("uri")

    async def _send_local_file(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str],
        metadata: Optional[Dict[str, Any]],
        file_name: Optional[str] = None,
    ) -> SendResult:
        import mimetypes

        p = Path(file_path)
        if not p.exists():
            logger.warning("Zulip: local file not found, skipping: %s", file_path)
            return SendResult(success=True, message_id=None)
        fname = file_name or p.name
        content_type = mimetypes.guess_type(fname)[0] or "application/octet-stream"
        uri = await self._upload_file(p.read_bytes(), fname, content_type)
        if not uri:
            return SendResult(success=False, error="file upload failed")
        link = f"[{fname}]({uri})"
        return await self.send(chat_id, f"{caption}\n{link}".strip(), metadata=metadata)

    async def _send_url_as_file(
        self,
        chat_id: str,
        url: str,
        caption: Optional[str],
        metadata: Optional[Dict[str, Any]],
    ) -> SendResult:
        from tools.url_safety import is_safe_url

        if not is_safe_url(url):
            logger.warning("Zulip: blocked unsafe URL (SSRF protection)")
            return await self.send(chat_id, f"{caption or ''}\n{url}".strip(), metadata=metadata)

        import aiohttp

        fname = url.rsplit("/", 1)[-1].split("?")[0] or "attachment"
        try:
            async with self._session.get(
                url, timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                if resp.status >= 400:
                    return await self.send(chat_id, f"{caption or ''}\n{url}".strip(), metadata=metadata)
                data = await resp.read()
                content_type = resp.content_type or "application/octet-stream"
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.warning("Zulip: failed to download %s: %s", url, exc)
            return await self.send(chat_id, f"{caption or ''}\n{url}".strip(), metadata=metadata)

        uri = await self._upload_file(data, fname, content_type)
        if not uri:
            return SendResult(success=False, error="file upload failed")
        link = f"[{fname}]({uri})"
        return await self.send(chat_id, f"{caption}\n{link}".strip(), metadata=metadata)

    async def _stream_name_for_id(self, stream_id: int) -> str:
        subs = await self._api_get("users/me/subscriptions")
        if subs.get("result") == "success":
            for s in subs.get("subscriptions", []):
                if s.get("stream_id") == stream_id:
                    return s.get("name", str(stream_id))
        return str(stream_id)

    # ------------------------------------------------------------------
    # Event polling
    # ------------------------------------------------------------------

    async def _poll_loop(self) -> None:
        """Register an event queue and long-poll for messages, reconnecting."""
        delay = _RECONNECT_BASE_DELAY
        while not self._closing:
            try:
                await self._register_and_poll()
                delay = _RECONNECT_BASE_DELAY
            except asyncio.CancelledError:
                return
            except Exception as exc:
                if self._closing:
                    return
                logger.warning("Zulip poll error: %s - reconnecting in %.0fs", exc, delay)
            if self._closing:
                return
            import random

            jitter = delay * _RECONNECT_JITTER * random.random()
            await asyncio.sleep(delay + jitter)
            delay = min(delay * 2, _RECONNECT_MAX_DELAY)

    async def _register_and_poll(self) -> None:
        """Register a fresh event queue and poll until it expires or breaks."""
        reg = await self._api_post_form(
            "register",
            {"event_types": json_dumps(["message"])},
        )
        if reg.get("result") != "success":
            raise RuntimeError(f"register failed: {reg.get('msg', 'unknown')}")
        self._queue_id = reg["queue_id"]
        self._last_event_id = reg.get("last_event_id", -1)
        logger.info("Zulip: event queue registered (queue_id=%s)", self._queue_id)

        import aiohttp

        while not self._closing:
            events_url = f"{self._site}/api/v1/events"
            try:
                async with self._session.get(
                    events_url,
                    headers=self._headers(),
                    params={
                        "queue_id": self._queue_id,
                        "last_event_id": str(self._last_event_id),
                    },
                    # Long-poll: Zulip holds the request open for up to ~5 minutes.
                    timeout=aiohttp.ClientTimeout(total=330),
                ) as resp:
                    body = await resp.json()
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if self._closing:
                    return
                raise RuntimeError(f"events poll failed: {exc}")

            if body.get("result") == "success":
                for event in body.get("events", []):
                    self._last_event_id = max(
                        self._last_event_id, event.get("id", self._last_event_id)
                    )
                    await self._handle_event(event)
            elif body.get("code") == "BAD_EVENT_QUEUE_ID":
                # Queue expired - drop out and re-register.
                logger.info("Zulip: event queue expired, re-registering")
                return
            else:
                logger.warning("Zulip: events returned %s", body.get("msg", "")[:200])
                await asyncio.sleep(1)

    async def _handle_event(self, event: Dict[str, Any]) -> None:
        """Dispatch a single Zulip event to the gateway."""
        if event.get("type") != "message":
            return
        msg = event.get("message") or {}
        if not msg:
            return

        # Skip our own messages to avoid reply loops.
        if self._bot_user_id is not None and msg.get("sender_id") == self._bot_user_id:
            return

        is_stream = msg.get("type") == "stream"
        if is_stream:
            stream_id = msg.get("stream_id")
            # Restrict to the configured stream when one is set.
            if self._anointed_stream_id is not None and stream_id != self._anointed_stream_id:
                return
            topic = msg.get("subject") or ""
            if not topic:
                return
            chat_jid = f"zulip:{stream_id}:{topic}"
            chat_type = "group"
        else:
            chat_jid = f"zulip:dm:{msg.get('sender_id')}"
            chat_type = "dm"

        # Mention gating for stream messages. DMs always pass through.
        text = html_to_text(msg.get("content", ""))
        if is_stream:
            extra = self.config.extra
            require_mention = str(
                extra_or_secret(extra, "require_mention", "ZULIP_REQUIRE_MENTION", "true", blank_is_unset=False)
            ).lower() not in {"false", "0", "no"}
            free_topics_raw = extra_or_secret(
                extra, "free_response_topics", "ZULIP_FREE_RESPONSE_TOPICS", "", blank_is_unset=False)
            if isinstance(free_topics_raw, list):
                free_topics_raw = ",".join(str(t) for t in free_topics_raw)
            free_topics = {t.strip() for t in str(free_topics_raw).split(",") if t.strip()}
            mention_tokens = self._mention_tokens()
            has_mention = is_bot_mentioned(
                event.get("flags"),
                msg.get("content", ""),
                text,
                self._bot_user_id,
                mention_tokens,
            )
            if require_mention and topic not in free_topics and not has_mention:
                logger.debug("Zulip: skipping stream message without @mention (%s)", chat_jid)
                return
            if has_mention:
                text = strip_mention(text, mention_tokens)

        msg_id = str(msg.get("id", "")) if msg.get("id") is not None else None
        if msg_id and self._dedup.is_duplicate(msg_id):
            return

        # Download any attachments so downstream tools can read them without
        # the bot's Basic auth headers.
        media_urls: List[str] = []
        media_types: List[str] = []
        for upload_path, filename in extract_attachment_links(msg.get("content", "")):
            try:
                file_bytes = await self._download_upload(upload_path)
                if file_bytes is None:
                    continue
                ext = Path(filename).suffix or Path(upload_path).suffix or ""
                from gateway.platforms.base import (
                    cache_image_from_bytes,
                    cache_audio_from_bytes,
                    cache_document_from_bytes,
                )
                if ext.lower() in {".png", ".jpg", ".jpeg", ".gif", ".webp"}:
                    media_urls.append(cache_image_from_bytes(file_bytes, ext or ".png"))
                    media_types.append("image/" + ext.lstrip("."))
                elif ext.lower() in {".ogg", ".mp3", ".m4a", ".wav"}:
                    media_urls.append(cache_audio_from_bytes(file_bytes, ext or ".ogg"))
                    media_types.append("audio/" + ext.lstrip("."))
                else:
                    media_urls.append(cache_document_from_bytes(file_bytes, filename))
                    media_types.append("application/octet-stream")
            except Exception as exc:
                logger.warning("Zulip: failed to download attachment %s: %s", upload_path, exc)

        msg_type = MessageType.TEXT
        if media_types and any(m.startswith("image/") for m in media_types):
            msg_type = MessageType.PHOTO
        elif media_types:
            msg_type = MessageType.DOCUMENT

        source = self.build_source(
            chat_id=chat_jid,
            chat_type=chat_type,
            user_id=str(msg.get("sender_id", "")) or None,
            user_name=msg.get("sender_full_name", ""),
            message_id=msg_id,
        )

        event_obj = MessageEvent(
            text=text,
            message_type=msg_type,
            source=source,
            raw_message=msg,
            message_id=msg_id,
            media_urls=media_urls if media_urls else None,
            media_types=media_types if media_types else None,
        )
        await self.handle_message(event_obj)

    def _mention_tokens(self) -> List[str]:
        """Strings the bot can be @-mentioned with."""
        tokens: List[str] = []
        username = (self._email.split("@", 1)[0] if self._email else "") or ""
        if username:
            tokens.append(username)
        if self._bot_full_name:
            tokens.append(self._bot_full_name)
        if self._bot_user_id is not None:
            tokens.append(str(self._bot_user_id))
        return tokens

    async def _download_upload(self, upload_path: str) -> Optional[bytes]:
        """Download a /user_uploads/ path using the bot's Basic auth."""
        import aiohttp

        url = upload_path if upload_path.startswith("http") else f"{self._site}{upload_path}"
        async with self._session.get(
            url,
            headers=self._headers(),
            timeout=aiohttp.ClientTimeout(total=60),
        ) as resp:
            if resp.status >= 400:
                logger.warning("Zulip: failed to download %s: HTTP %s", upload_path, resp.status)
                return None
            data = await resp.read()
            return data


def json_dumps(value: Any) -> str:
    """Serialise to JSON without importing a heavy dependency at module top."""
    import json

    return json.dumps(value)


def encode_form_params(params: Dict[str, Any]) -> Dict[str, str]:
    """Prepare a Zulip message payload for form-urlencoded POST.

    Zulip's REST API expects the DM ``to`` field as a JSON array string
    (e.g. ``"[123]"``), not as a repeated form field. Stream ``to`` is a bare
    integer. This helper JSON-encodes any list value so aiohttp's form encoder
    sends the right shape.

    @param params - The payload from build_send_params.
    @returns A flat dict safe to pass as aiohttp ``data=``.
    """
    encoded: Dict[str, str] = {}
    for key, value in params.items():
        if isinstance(value, (list, dict)):
            encoded[key] = json_dumps(value)
        else:
            encoded[key] = str(value)
    return encoded


# ---------------------------------------------------------------------------
# Standalone send (out-of-process cron delivery)
# ---------------------------------------------------------------------------


async def _standalone_send(
    pconfig,
    chat_id: str,
    message: str,
    *,
    thread_id: Optional[str] = None,
    media_files: Optional[list] = None,
    force_document: bool = False,
) -> Dict[str, Any]:
    """Send via the Zulip REST API without a live gateway adapter.

    Used by cron jobs that run out-of-process. ``force_document`` is accepted
    for signature parity with other standalone senders but unused - Zulip
    stores every upload as a generic attachment.
    """
    try:
        import aiohttp
    except ImportError:
        return {"error": "aiohttp not installed. Run: pip install aiohttp"}

    site = (getattr(pconfig, "extra", {}) or {}).get("site") or get_scoped_secret("ZULIP_SITE", "")
    site = site.rstrip("/")
    email = (getattr(pconfig, "extra", {}) or {}).get("bot_email") or get_scoped_secret("ZULIP_BOT_EMAIL", "")
    api_key = getattr(pconfig, "token", None) or get_scoped_secret("ZULIP_BOT_API_KEY", "")
    if not site or not email or not api_key:
        return {"error": "Zulip standalone send: ZULIP_SITE, ZULIP_BOT_EMAIL, and ZULIP_BOT_API_KEY must all be set"}

    auth = "Basic " + base64.b64encode(f"{email}:{api_key}".encode("utf-8")).decode("ascii")
    headers = {"Authorization": auth}

    params = build_send_params(chat_id, message)
    if not params:
        return {"error": f"Invalid Zulip JID: {chat_id}"}

    media_files = media_files or []
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
            # Upload any media first and append their links to the message.
            if media_files:
                for media in media_files:
                    file_path = media.get("path") if isinstance(media, dict) else media
                    if not file_path or not os.path.exists(file_path):
                        continue
                    import mimetypes

                    fname = os.path.basename(file_path)
                    ct = mimetypes.guess_type(fname)[0] or "application/octet-stream"
                    form = aiohttp.FormData()
                    form.add_field("file", open(file_path, "rb").read(), filename=fname, content_type=ct)
                    async with session.post(
                        f"{site}/api/v1/user_uploads", headers=headers, data=form
                    ) as up_resp:
                        up_body = await up_resp.json()
                        if up_body.get("result") != "success":
                            return {"error": f"Zulip upload failed: {up_body.get('msg')}"}
                        params["content"] += f"\n[{fname}]({up_body.get('uri')})"

            async with session.post(
                f"{site}/api/v1/messages", headers=headers, data=encode_form_params(params)
            ) as resp:
                body = await resp.json()
                if body.get("result") != "success":
                    return {"error": f"Zulip API error: {body.get('msg', 'unknown')}"}
                return {
                    "success": True,
                    "platform": "zulip",
                    "chat_id": chat_id,
                    "message_id": str(body.get("id")) if body.get("id") is not None else None,
                }
    except aiohttp.ClientError as exc:
        return {"error": f"Zulip send failed (network): {exc}"}
    except Exception as exc:
        return {"error": f"Zulip send failed: {exc}"}


# ---------------------------------------------------------------------------
# is_connected probe
# ---------------------------------------------------------------------------


# Zulip is connected when site, bot email, and API key are all set.
_is_connected = env_is_connected("ZULIP_SITE", "ZULIP_BOT_EMAIL", "ZULIP_BOT_API_KEY")


# ---------------------------------------------------------------------------
# YAML -> env config bridge
# ---------------------------------------------------------------------------


def _apply_yaml_config(yaml_cfg: dict, zulip_cfg: dict) -> Optional[dict]:
    """Translate config.yaml zulip: keys into ZULIP_* env vars and PlatformConfig.extra.

    Env wins over YAML; the env write is skipped under a multiplexed secondary
    profile's scope (it would leak that profile's policy process-wide), so the
    YAML values are also seeded into ``extra`` for each profile's adapter.
    """
    return apply_yaml_bridge(zulip_cfg, (
        ("require_mention", "ZULIP_REQUIRE_MENTION", "lower"),
        ("free_response_topics", "ZULIP_FREE_RESPONSE_TOPICS", "csv"),
        ("stream", "ZULIP_STREAM", "str"),
    ))


# ---------------------------------------------------------------------------
# Plugin registration
# ---------------------------------------------------------------------------


def _build_adapter(config):
    """Factory wrapper that constructs ZulipAdapter from a PlatformConfig."""
    return ZulipAdapter(config)


def register(ctx) -> None:
    """Plugin entry point - called by the Hermes plugin system."""
    ctx.register_platform(
        name="zulip",
        label="Zulip",
        adapter_factory=_build_adapter,
        check_fn=check_zulip_requirements,
        is_connected=_is_connected,
        required_env=["ZULIP_SITE", "ZULIP_BOT_EMAIL", "ZULIP_BOT_API_KEY"],
        install_hint="pip install aiohttp",
        apply_yaml_config_fn=_apply_yaml_config,
        allowed_users_env="ZULIP_ALLOWED_USERS",
        allow_all_env="ZULIP_ALLOW_ALL_USERS",
        cron_deliver_env_var="ZULIP_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        max_message_length=MAX_MESSAGE_LENGTH,
        emoji="💬",
        allow_update_command=True,
    )
