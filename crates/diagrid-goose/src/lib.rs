//! Durable Dapr Workflow integration for [Goose](https://github.com/aaif-goose/goose).
//!
//! **Scaffold only.** This crate does not yet wrap any Goose operations.
//! Goose's actual agent loop (tool calling, extensions/MCP, session state
//! machine) lives in the `goose-agent` crate inside the `aaif-goose/goose`
//! monorepo, which is not published to crates.io as an importable library —
//! how to depend on it (path/git dependency against that monorepo, or
//! vendoring specific crates) is still an open decision and isn't wired up
//! here yet.
//!
//! What this crate does validate today is the foundation everything else
//! will sit on: the `dapr` crate's Workflow module (activities, retry
//! policies, and — critically — crash recovery when the process is killed
//! mid-activity). See `examples/crash_recovery.rs` for a runnable,
//! end-to-end demonstration against local Dapr.
