# Durable Workflows for AI agents (Rust)

**Make your AI agents resilient to failure and outages**

This is the Rust counterpart to [`python-ai`](https://github.com/diagridio/python-ai), [`dotnet-ai`](https://github.com/diagridio/dotnet-ai), and [`typescript-ai`](https://github.com/diagridio/typescript-ai): an extension SDK for the open-source [Dapr](https://github.com/dapr/dapr) project that wraps AI agent frameworks in Dapr Workflows so agents can recover from failures, persist state across restarts, and scale effectively — for agent frameworks whose runtime lives natively in Rust rather than being callable from Python/.NET/TypeScript.

## Status

Early scaffold. No crates published yet.

Built on [`dapr/rust-sdk`](https://github.com/dapr/rust-sdk)'s Workflow module (Alpha), which has been validated locally against Dapr for activity execution, retry policies, and crash recovery (process killed mid-activity resumes correctly on restart).

First planned integration target: [Goose](https://github.com/aaif-goose/goose), whose agent loop (tool calling, extensions/MCP, session state machine) lives entirely in Rust.

## Community

Have questions, hit a bug, or want to share what you're building? Join the [Diagrid Community Discord](https://diagrid.ws/diagrid-community) to connect with the team and other users.
