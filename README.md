# qq-codex-sync

Selective personal QQ chat archiving for Codex on macOS.

**Status:** experimental MVP; not yet verified against live NapCat. Never commit real chat logs or tokens.

## Capabilities

- NapCat OneBot v11 WebSocket incoming / sent message listener.
- Exact allowlist of private contacts and QQ groups.
- SQLite storage, message deduplication, and searchable text.
- A read-only stdio MCP endpoint for Codex: `search_messages`, `get_message_context`, `list_conversations`.
- Import a file of OneBot JSON or JSONL events, scoped to the same allowlist.
- Automatic reconnection, **not historical backfill**. Messages received while disconnected may be missed.

## Requirements

macOS (Apple Silicon or Intel), Rust stable, a running NapCat installation with a loopback-only OneBot WebSocket server. Third-party QQ integration may break with QQ updates or carry account risk.

## Setup

```sh
git clone https://github.com/SoloJiang/qq-codex-sync.git
cd qq-codex-sync
cp config.example.toml config.toml
# Edit the group/private identifiers in config.toml.
export NAPCAT_TOKEN='YOUR_ONEBOT_WEBSOCKET_TOKEN'
cargo build --release
./target/release/qq-codex-sync --config config.toml init
./target/release/qq-codex-sync --config config.toml sync
```

Choose the exact QQ IDs to archive. Keep `config.toml`, `data/`, and secrets local; these are ignored by git.

## Codex MCP

In `~/.codex/config.toml`:

```toml
[mcp_servers.qq_search]
command = "/ABSOLUTE/PATH/qq-codex-sync/target/release/qq-codex-sync"
args = ["--config", "/ABSOLUTE/PATH/qq-codex-sync/config.toml", "mcp"]
```

The MCP process reads the same SQLite database that `sync` updates. Run `sync` separately or with macOS `launchd` using the template in `examples/`. A token in a launchd plist is sensitive; restrict file permissions (chmod 600), or use a safer local secret loading arrangement.

## Other commands

```sh
qq-codex-sync --config config.toml stats
qq-codex-sync --config config.toml import path/to/onebot-events.jsonl
```

The `import` command expects OneBot-shaped event objects, not arbitrary QQ's encrypted database or built-in backup formats.

## Limitations

- This is **not** a complete historical QQ export. Only observed or explicitly imported OneBot events are indexed.
- No automatic gap filling or scheduled historical pull yet.
- Text-only keyword searches use SQLite `LIKE`, not FTS5; suitable for a small first-version archive, and it can match Chinese substrings.
- Plaintext database and raw JSON are stored locally; secure Mac user account and disk.
- Only enable local `127.0.0.1` connections. Do not expose NapCat or MCP on a public interface.
- Verify the OneBot event shape and QQ's permissions before relying on the tool.

## Next steps

Targeted historical pagination, persisted per-chat sync cursors, automatic gap detection/backfill, FTS5, optional retention/delete commands, and integration tests with actual NapCat captures.
