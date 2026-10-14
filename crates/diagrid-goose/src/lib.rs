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
//! worker is available, and a brand new `goose acp` process resumes the same
//! on-disk session. Turns that already completed are replayed from workflow
//! history, not re-run; the interrupted one picks up from what Goose's
//! session holds — its prompt is never shown to the model twice, and an
//! attempt that finished before its result was recorded isn't run again
//! (see [`turn`]'s "Retried turns"). See `examples/goose_crash_recovery.rs`
//! for this scenario end to end against local Dapr and a real `goose`
//! binary (v1.54.0), including a real `kill -TERM` sent to the worker
//! process mid-turn.
//!
//! Goose keeps those sessions in its own SQLite store (`GOOSE_PATH_ROOT`,
//! by default under `~/.local/share/goose`), and its session ids are only
//! unique within one store — so the store has to outlive the worker process,
//! and a turn has to resume against the same store its session started in.
//!
//! # Usage
//!
//! Register the workflow, start a worker, then schedule a run — one message
//! per turn — and wait for its result. The app runs next to a Dapr sidecar
//! (`dapr run --app-id my-agent -- cargo run`), and Goose uses whatever
//! model provider it is configured for. `examples/quickstart.rs` is a
//! complete, runnable version; the repository README has the setup steps.
//!
//! ```no_run
//! # async fn run() -> anyhow::Result<()> {
//! use std::time::Duration;
//!
//! use dapr::workflow::{FetchOptions, RuntimeStatus, ScheduleOptions, WorkflowClient};
//! use diagrid_goose::{GooseWorkflowInput, GooseWorkflowOutput, WORKFLOW_NAME};
//!
//! let mut client = WorkflowClient::new().await?;
//! diagrid_goose::register(client.registry_mut());
//! let worker = client.start_worker().await?;
//!
//! let input = GooseWorkflowInput {
//!     program: "goose".to_string(),
//!     args: vec!["acp".to_string()],
//!     // Extra environment for Goose, e.g. provider settings.
//!     env: vec![],
//!     // Where Goose works. Its tool calls are approved automatically, so it
//!     // may change files here.
//!     cwd: std::env::temp_dir(),
//!     // One message per turn, all in the same Goose session.
//!     tasks: vec![
//!         "Write a haiku about durable execution.".to_string(),
//!         "Now translate it into French.".to_string(),
//!     ],
//! };
//! let instance_id = client
//!     .schedule_workflow(WORKFLOW_NAME, ScheduleOptions::new().try_with_input(input)?)
//!     .await?;
//!
//! let done = client
//!     .wait_for_workflow_completion_with_options(
//!         &instance_id,
//!         FetchOptions::new().with_fetch_payloads(true),
//!         Some(Duration::from_secs(600)),
//!     )
//!     .await?;
//! anyhow::ensure!(
//!     done.runtime_status == RuntimeStatus::Completed,
//!     "workflow ended {:?}: {:?}",
//!     done.runtime_status,
//!     done.failure_details
//! );
//! let output: GooseWorkflowOutput =
//!     serde_json::from_str(done.serialized_output.as_deref().unwrap_or_default())?;
//! println!("{}", output.final_response);
//!
//! worker.shutdown().await?;
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
