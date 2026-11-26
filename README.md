# hermes-zulip

A [Zulip](https://zulip.com) gateway platform plugin for
[Hermes Agent](https://github.com/NousResearch/hermes-agent). It connects a
Zulip bot to Hermes so you can talk to your agent in Zulip streams, topics and
direct messages.

- Works with Zulip Cloud and self-hosted servers through the v1 REST API and
  the long-polling event queue. Its only dependency is `aiohttp`; no Zulip SDK
  is needed.
- Each stream topic and each direct-message partner gets its own Hermes
  session.
- Can be limited to a single stream.
- Requires an @-mention in streams by default, with opt-out topics. Direct
  messages always reach the agent.
- Downloads attachments sent to the bot and uploads files the agent sends.
- Splits replies longer than Zulip's 10,000-character limit.
- Delivers cron output to a home channel.
- Works under a multi-profile gateway (`gateway.multiplex_profiles`): each
  profile signs in as its own bot.

## Requirements

- Hermes Agent 0.21.6 or later
- A Zulip [bot account](https://zulip.com/help/add-a-bot-or-integration)
  (a "Generic bot" is enough), with its email and API key

## Installation

```sh
hermes plugins install johngrimes/hermes-zulip --enable
```

Hermes asks you to approve installing `aiohttp` (if it isn't already present)
and prompts for the required settings. To use the plugin in another profile,
install it there too (`hermes -p <profile> plugins install …`).

## Configuration

Set these in the profile's `.env` (or any secret source Hermes supports):

| Variable | Required | Description |
| --- | --- | --- |
| `ZULIP_SITE` | yes | Organisation URL, e.g. `https://example.zulipchat.com` |
| `ZULIP_BOT_EMAIL` | yes | Bot account email |
| `ZULIP_BOT_API_KEY` | yes | Bot API key |
| `ZULIP_STREAM` | no | Name of the only stream the bot responds in. Without it, the bot responds in every stream it is subscribed to. |
| `ZULIP_ALLOWED_USERS` | no | Comma-separated Zulip user IDs allowed to talk to the bot |
| `ZULIP_ALLOW_ALL_USERS` | no | `true` lets any user talk to the bot (development only) |
| `ZULIP_HOME_CHANNEL` | no | Chat ID for cron output and Hermes status notices, such as the startup notice (see below) |
| `ZULIP_REQUIRE_MENTION` | no | Require an @-mention in streams (default `true`) |
| `ZULIP_FREE_RESPONSE_TOPICS` | no | Comma-separated topics where no @-mention is needed |

The last three non-secret settings can go in `config.yaml` instead; environment
variables take precedence:

```yaml
zulip:
  stream: hermes
  require_mention: true
  free_response_topics:
    - chat
```

The bot answers to its username (the part of its email before `@`), its full
name and its user ID.

### Chat IDs

Hermes addresses Zulip conversations with these chat IDs, which you can use
for `ZULIP_HOME_CHANNEL` and cron delivery targets:

| Conversation | Chat ID |
| --- | --- |
| Stream topic | `zulip:<stream_id>:<topic>` |
| Direct message | `zulip:dm:<user_id>` |

## Updating

```sh
hermes plugins update zulip-platform
```

## Development

The tests import Hermes's gateway modules, so run them inside a Hermes
checkout's environment ([uv](https://docs.astral.sh/uv/) and Python 3.14):

```sh
git clone https://github.com/NousResearch/hermes-agent.git ../hermes-agent
uv sync --project ../hermes-agent --frozen
uv run --project ../hermes-agent --frozen --with pytest pytest
```

CI runs the tests and `hermes plugins validate --install-deps` against the
oldest supported Hermes release (0.21.6) and Hermes `main`.

## License

[MIT](LICENSE)
