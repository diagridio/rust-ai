//! Durable Dapr Workflow integration for [Goose](https://github.com/aaif-goose/goose).
//!
//! Goose's agent loop (tool calling, extensions/MCP, session state machine)
//! lives entirely in Rust, but not as an importable library — `goose-agent`
//! and friends are internal crates of the `aaif-goose/goose` monorepo, not
//! published to crates.io. This crate does not depend on them, vendor them,
//! or require any change to Goose itself. Instead it drives an unmodified
//! `goose acp` (or `goose serve`) process entirely through Goose's stable,
//! already-published [Agent Client Protocol][acp] interface — the same
//! interface Zed and other ACP clients use, and the same one Goose's own
//! roaming feature uses to talk to *other* agents
//! (`crates/goose-cli/src/commands/roam_client.rs` upstream).
//!
//! [acp]: https://agentclientprotocol.com/
//!
//! # Durability model
//!
//! One Dapr activity = one Goose turn (one `session/prompt` round trip
//! against a freshly spawned process). [`workflow::goose_workflow`] is the
//! orchestrator: it loops over the caller's task list, feeding the
//! `session_id` returned by each turn into the next one's input via Goose's
//! own `session/load` support (Goose advertises `loadSession: true` and
//! persists sessions to disk itself — nothing here does).
//!
//! If the worker dies mid-turn, there is no shared process or connection
//! state to strand: Dapr redispatches that activity from scratch once a
//! worker is available, a brand new `goose acp` process resumes the same
//! on-disk session, and re-sends only the interrupted turn. Turns that
//! already completed are replayed from workflow history, not re-run. See
//! `examples/goose_crash_recovery.rs` for this scenario end to end against
//! local Dapr and a real `goose` binary, including a real `kill -TERM`
//! sent to the worker process mid-turn.
//!
//! # Usage
//!
//! ```no_run
//! # async fn run() -> anyhow::Result<()> {
//! use dapr::workflow::WorkflowClient;
//!
//! let mut client = WorkflowClient::new().await?;
//! diagrid_goose::register(client.registry_mut());
//! let _worker = client.start_worker().await?;
//!
//! let input = diagrid_goose::GooseWorkflowInput {
//!     program: "goose".to_string(),
//!     args: vec!["acp".to_string()],
//!     env: vec![],
//!     cwd: std::env::current_dir()?,
//!     tasks: vec!["What is 2 + 2?".to_string()],
//! };
//! # Ok(())
//! # }
//! ```

pub mod turn;
pub mod workflow;

use dapr::workflow::RegistryExt;

pub use turn::{RunTurnInput, RunTurnOutput};
pub use workflow::{GooseWorkflowInput, GooseWorkflowOutput, ACTIVITY_NAME, WORKFLOW_NAME};

/// Register the Goose workflow and its turn activity on a Dapr `Registry`.
///
/// ```no_run
/// # async fn run() -> anyhow::Result<()> {
/// use dapr::workflow::WorkflowClient;
/// let mut client = WorkflowClient::new().await?;
/// diagrid_goose::register(client.registry_mut());
/// # Ok(())
/// # }
/// ```
pub fn register(registry: &mut dapr::workflow::Registry) {
    registry.add_workflow(WORKFLOW_NAME, workflow::goose_workflow);
    registry.add_activity(ACTIVITY_NAME, workflow::run_goose_turn_activity);
}
