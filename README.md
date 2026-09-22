# Durable Workflows for AI agents (Rust)

**Make your AI agents resilient to failure and outages**

This is the Rust counterpart to [`python-ai`](https://github.com/diagridio/python-ai), [`dotnet-ai`](https://github.com/diagridio/dotnet-ai), and [`typescript-ai`](https://github.com/diagridio/typescript-ai): an extension SDK for the open-source [Dapr](https://github.com/dapr/dapr) project that wraps AI agent frameworks in Dapr Workflows so agents can recover from failures, persist state across restarts, and scale effectively — for agent frameworks whose runtime lives natively in Rust rather than being callable from Python/.NET/TypeScript.

## Status

No crates published yet. First integration, `diagrid-goose`, is implemented and validated locally against Dapr and a real `goose` binary — see [`crates/diagrid-goose`](crates/diagrid-goose).

Built on [`dapr/rust-sdk`](https://github.com/dapr/rust-sdk)'s Workflow module (Alpha), pinned to the exact published `dapr = "0.19.0"` release on crates.io — validated locally for activity execution, retry policies, and crash recovery (process killed mid-activity resumes correctly on restart).

### `diagrid-goose`

[Goose](https://github.com/aaif-goose/goose)'s agent loop (tool calling, extensions/MCP, session state machine) lives entirely in Rust, but not as an importable library — `goose-agent` and friends are internal crates of the `aaif-goose/goose` monorepo, not published to crates.io. `diagrid-goose` depends on none of them and requires no change to Goose itself: it drives an unmodified `goose acp` process purely over Goose's stable, already-published [Agent Client Protocol](https://agentclientprotocol.com/) interface.

One Dapr activity = one Goose turn (one `session/prompt` round trip against a freshly spawned process). Session continuity across turns rides Goose's own `session/load` support, so each turn is an independent, freshly spawned activity with no shared process or connection state that a crash could strand. Verified end to end against local Dapr with a real `goose` binary: `kill -TERM` sent to the worker mid-turn, a fresh process resumes the same Goose session and completes — see `crates/diagrid-goose/examples/goose_crash_recovery.rs`.

## Community

Have questions, hit a bug, or want to share what you're building? Join the [Diagrid Community Discord](https://diagrid.ws/diagrid-community) to connect with the team and other users.
