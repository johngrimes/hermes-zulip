"""Tests for the Zulip platform adapter plugin.

Covers the pure helper functions that drive JID routing, outbound payload
construction, inbound HTML normalisation, attachment link extraction, and
mention stripping. These are the highest-correctness surfaces of the
adapter - the bits that, if wrong, route messages to the wrong place or
drop attachments silently.

Author: John Grimes
"""

import importlib.util
import pathlib

import pytest

# The plugin is loaded by Hermes dynamically by file path (it is not on the
# normal package path), so resolve the adapter module the same way here.
_ADAPTER_PATH = (
    pathlib.Path(__file__).parent / "adapter.py"
)
_spec = importlib.util.spec_from_file_location("zulip_adapter_under_test", _ADAPTER_PATH)
adapter = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(adapter)

MAX_MESSAGE_LENGTH = adapter.MAX_MESSAGE_LENGTH
build_send_params = adapter.build_send_params
encode_form_params = adapter.encode_form_params
extract_attachment_links = adapter.extract_attachment_links
html_to_text = adapter.html_to_text
parse_jid = adapter.parse_jid
strip_mention = adapter.strip_mention
is_bot_mentioned = adapter.is_bot_mentioned
parse_target_ref = getattr(adapter, "parse_target_ref", None)


class TestParseJid:
    def test_stream_jid_parses_to_stream_id_and_topic(self):
        parsed = parse_jid("zulip:42:general")
        assert parsed == {"type": "stream", "stream_id": 42, "topic": "general"}

    def test_dm_jid_parses_to_user_id(self):
        parsed = parse_jid("zulip:dm:7")
        assert parsed == {"type": "dm", "user_id": 7}

    def test_stream_jid_preserves_topic_with_colons(self):
        # Topics may contain colons (e.g. "release: 1.2"). Only the first
        # colon after the stream id splits the topic.
        parsed = parse_jid("zulip:42:release: 1.2")
        assert parsed == {"type": "stream", "stream_id": 42, "topic": "release: 1.2"}

    def test_non_zulip_jid_returns_none(self):
        assert parse_jid("telegram:12345") is None

    def test_missing_prefix_returns_none(self):
        assert parse_jid("5:general") is None

    def test_non_numeric_stream_id_returns_none(self):
        assert parse_jid("zulip:abc:general") is None

    def test_non_numeric_dm_user_id_returns_none(self):
        assert parse_jid("zulip:dm:abc") is None

    def test_empty_topic_returns_none(self):
        assert parse_jid("zulip:42:") is None

    def test_dm_without_user_id_returns_none(self):
        assert parse_jid("zulip:dm:") is None


class TestParseTargetRef:
    """Hermes strips the platform prefix from delivery targets ("zulip:42:general"
    reaches the plugin as "42:general"), so the parser restores the full JID."""

    def test_stream_topic_without_prefix(self):
        assert parse_target_ref("659014:Morning brief") == ("zulip:659014:Morning brief", None)

    def test_dm_without_prefix(self):
        assert parse_target_ref("dm:7") == ("zulip:dm:7", None)

    def test_full_jid_passes_through(self):
        assert parse_target_ref("zulip:42:general") == ("zulip:42:general", None)

    def test_topic_with_colons(self):
        assert parse_target_ref("42:a:b") == ("zulip:42:a:b", None)

    def test_surrounding_whitespace_is_ignored(self):
        assert parse_target_ref("  42:general ") == ("zulip:42:general", None)

    def test_channel_name_is_left_to_core_resolution(self):
        assert parse_target_ref("briefs") is None

    def test_missing_topic_is_not_a_target(self):
        assert parse_target_ref("42:") is None


class TestBuildSendParams:
    def test_stream_payload_uses_stream_id_and_topic(self):
        params = build_send_params("zulip:42:general", "hello")
        assert params == {
            "type": "stream",
            "to": 42,
            "topic": "general",
            "content": "hello",
        }

    def test_dm_payload_uses_direct_type_and_user_list(self):
        params = build_send_params("zulip:dm:7", "hello")
        assert params == {
            "type": "direct",
            "to": [7],
            "content": "hello",
        }

    def test_invalid_jid_returns_none(self):
        assert build_send_params("telegram:1", "hello") is None


class TestHtmlToText:
    def test_strips_simple_tags(self):
        assert html_to_text("<p>hello</p>") == "hello"

    def test_converts_breaks_and_paragraphs_to_newlines(self):
        assert html_to_text("<p>one</p><br><p>two</p>") == "one\ntwo"

    def test_decodes_entities(self):
        assert html_to_text("a &amp; b &lt; c") == "a & b < c"

    def test_collapses_whitespace(self):
        assert html_to_text("<p>hello   world</p>") == "hello world"

    def test_preserves_code_content(self):
        # Code blocks should keep their text content intact.
        assert html_to_text("<pre>line1\nline2</pre>") == "line1\nline2"

    def test_empty_input_returns_empty(self):
        assert html_to_text("") == ""

    def test_user_mention_link_text_is_preserved(self):
        # Zulip renders @mentions as <a> links; the visible text matters.
        assert html_to_text('<p><span class="user-mention">@John</span> hi</p>') == "@John hi"


class TestExtractAttachmentLinks:
    def test_finds_user_upload_anchor(self):
        html = '<a href="/user_uploads/abc/file.txt">file.txt</a>'
        links = extract_attachment_links(html)
        assert links == [("/user_uploads/abc/file.txt", "file.txt")]

    def test_ignores_non_upload_links(self):
        html = '<a href="https://example.com/page">page</a>'
        assert extract_attachment_links(html) == []

    def test_finds_multiple_uploads(self):
        html = (
            '<a href="/user_uploads/a/1.png">1.png</a>'
            '<a href="/user_uploads/b/2.pdf">2.pdf</a>'
        )
        links = extract_attachment_links(html)
        assert links == [
            ("/user_uploads/a/1.png", "1.png"),
            ("/user_uploads/b/2.pdf", "2.pdf"),
        ]

    def test_absolute_upload_urls_are_kept(self):
        # Zulip sometimes returns absolute URLs for uploads.
        html = '<a href="https://example.zulipchat.com/user_uploads/x/y.bin">y.bin</a>'
        links = extract_attachment_links(html)
        assert len(links) == 1
        assert links[0][1] == "y.bin"

    def test_no_links_returns_empty(self):
        assert extract_attachment_links("plain text") == []


class TestStripMention:
    def test_strips_at_username(self):
        assert strip_mention("@hermes-bot hello", ["hermes-bot"]) == "hello"

    def test_strips_at_user_id(self):
        assert strip_mention("@12 hello", ["hermes-bot", "12"]) == "hello"

    def test_case_insensitive(self):
        assert strip_mention("@HERMES-BOT hi", ["hermes-bot"]) == "hi"

    def test_no_mention_returns_unchanged(self):
        assert strip_mention("hello there", ["hermes-bot"]) == "hello there"

    def test_strips_multiple_mentions(self):
        assert strip_mention("@hermes-bot @hermes-bot hi", ["hermes-bot"]) == "hi"

    def test_strips_full_name_with_spaces(self):
        assert strip_mention("@Hermes Bot what's next?", ["hermes-bot", "Hermes Bot"]) == "what's next?"

    def test_leaves_numbers_matching_user_id_untouched(self):
        # The bot's user ID is a mention token, but a bare number in the text
        # is not a mention and must reach the agent intact.
        assert strip_mention("What's on 12 Dec?", ["hermes-bot", "12"]) == "What's on 12 Dec?"

    def test_leaves_unprefixed_name_untouched(self):
        assert strip_mention("is hermes-bot up?", ["hermes-bot"]) == "is hermes-bot up?"

    def test_prefers_full_name_over_shorter_username(self):
        assert strip_mention("@Hermes Bot hi", ["hermes", "Hermes Bot"]) == "hi"

    def test_does_not_strip_a_longer_name(self):
        assert strip_mention("@hermes-bot2 hi", ["hermes-bot"]) == "@hermes-bot2 hi"


class TestIsBotMentioned:
    TOKENS = ["hermes-bot", "Hermes Bot", "12"]

    def test_mentioned_flag(self):
        assert is_bot_mentioned(["read", "mentioned"], "<p>hi</p>", "hi", 12, self.TOKENS)

    def test_user_mention_span_for_bot(self):
        html = '<p><span class="user-mention" data-user-id="12">@Hermes Bot</span> hi</p>'
        assert is_bot_mentioned([], html, "@Hermes Bot hi", 12, self.TOKENS)

    def test_user_mention_span_for_someone_else(self):
        html = '<p><span class="user-mention" data-user-id="99">@Alice</span> hi</p>'
        assert not is_bot_mentioned([], html, "@Alice hi", 12, self.TOKENS)

    def test_at_name_in_text(self):
        assert is_bot_mentioned([], "", "@hermes-bot hello", 12, self.TOKENS)

    def test_number_matching_user_id_is_not_a_mention(self):
        assert not is_bot_mentioned([], "<p>meet at 12 tomorrow</p>", "meet at 12 tomorrow", 12, self.TOKENS)

    def test_name_without_at_is_not_a_mention(self):
        assert not is_bot_mentioned([], "", "is hermes-bot up?", 12, self.TOKENS)

    def test_no_flags(self):
        assert not is_bot_mentioned(None, "", "hello", 12, self.TOKENS)


class TestEncodeFormParams:
    def test_stream_to_stays_as_integer_string(self):
        params = build_send_params("zulip:42:general", "hi")
        encoded = encode_form_params(params)
        assert encoded["to"] == "42"
        assert encoded["type"] == "stream"

    def test_dm_to_becomes_json_array_string(self):
        params = build_send_params("zulip:dm:7", "hi")
        encoded = encode_form_params(params)
        assert encoded["to"] == "[7]"
        assert encoded["type"] == "direct"

    def test_content_passes_through(self):
        params = build_send_params("zulip:42:general", "hello world")
        encoded = encode_form_params(params)
        assert encoded["content"] == "hello world"


class TestMaxMessageLength:
    def test_zulip_limit_is_reasonable(self):
        # Zulip's hard limit is 10000 chars; the adapter must chunk above it.
        assert MAX_MESSAGE_LENGTH <= 10000
        assert MAX_MESSAGE_LENGTH > 0


class _FakeResponse:
    def __init__(self, body):
        self._body = body

    async def json(self):
        return self._body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _FakeSession:
    calls = []

    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def post(self, url, headers=None, data=None):
        _FakeSession.calls.append({"url": url, "headers": headers, "data": data})
        return _FakeResponse({"result": "success", "id": 555})


class _PConfig:
    extra = {"site": "https://example.zulipchat.com/", "bot_email": "hermes-bot@example.zulipchat.com"}
    token = "secret-key"


class TestStandaloneSend:
    def test_sends_message_with_basic_auth(self, monkeypatch):
        import asyncio
        import base64
        import sys
        import types

        # aiohttp is an optional Hermes extra, so stub the parts the sender uses.
        fake_aiohttp = types.ModuleType("aiohttp")
        fake_aiohttp.ClientSession = _FakeSession
        fake_aiohttp.ClientTimeout = lambda **kwargs: None
        fake_aiohttp.ClientError = type("ClientError", (Exception,), {})
        monkeypatch.setitem(sys.modules, "aiohttp", fake_aiohttp)

        _FakeSession.calls = []
        result = asyncio.run(adapter._standalone_send(_PConfig(), "zulip:42:Morning brief", "hello"))

        assert result.get("success") is True, result
        assert result["message_id"] == "555"
        call = _FakeSession.calls[-1]
        assert call["url"] == "https://example.zulipchat.com/api/v1/messages"
        expected = base64.b64encode(b"hermes-bot@example.zulipchat.com:secret-key").decode("ascii")
        assert call["headers"]["Authorization"] == "Basic " + expected
        assert call["data"]["topic"] == "Morning brief"


class TestEnvEnablement:
    """ZULIP_HOME_CHANNEL must reach core as the platform's home channel, or
    startup notices and the no-home-channel check ignore it."""

    @pytest.fixture(autouse=True)
    def _creds(self, monkeypatch):
        monkeypatch.setenv("ZULIP_SITE", "https://example.zulipchat.com")
        monkeypatch.setenv("ZULIP_BOT_EMAIL", "bot@example.zulipchat.com")
        monkeypatch.setenv("ZULIP_BOT_API_KEY", "key")
        monkeypatch.delenv("ZULIP_HOME_CHANNEL", raising=False)
        monkeypatch.delenv("ZULIP_HOME_CHANNEL_NAME", raising=False)

    def test_home_channel_is_seeded(self, monkeypatch):
        monkeypatch.setenv("ZULIP_HOME_CHANNEL", "zulip:42:Hermes")
        seed = adapter._env_enablement()
        assert seed["home_channel"] == {"chat_id": "zulip:42:Hermes", "name": "Home"}

    def test_home_channel_name_is_used(self, monkeypatch):
        monkeypatch.setenv("ZULIP_HOME_CHANNEL", "zulip:42:Hermes")
        monkeypatch.setenv("ZULIP_HOME_CHANNEL_NAME", "logs")
        assert adapter._env_enablement()["home_channel"]["name"] == "logs"

    def test_no_home_channel_seeds_none(self):
        assert "home_channel" not in adapter._env_enablement()

    def test_unconfigured_returns_none(self, monkeypatch):
        monkeypatch.delenv("ZULIP_BOT_API_KEY")
        assert adapter._env_enablement() is None
