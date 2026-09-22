//! Dapr Workflow orchestrator + activity wrapping [`crate::turn::run_turn`].
//!
//! One Dapr activity == one Goose turn (one `session/prompt` round trip
//! against a freshly spawned `goose acp` process). The orchestrator is a
//! plain loop over the caller's task list, threading the `session_id`
//! returned by each activity into the next one's input — the same shape as
//! every other `diagrid.agent.*` runner's node-by-node execution loop, just
//! with "node" replaced by "turn".
//!
//! Durability: if the worker dies while a turn's activity is in flight, Dapr
//! redispatches that activity from scratch once a worker is available again
//! (see the crate's `examples/goose_crash_recovery.rs` for this exact
//! scenario, verified against local Dapr with a real `kill -TERM`). Turns
//! that already completed are replayed from history, not re-run — only the
//! interrupted turn re-executes, resuming the same Goose session via
//! `session/load` rather than starting the conversation over.

use std::path::PathBuf;

use dapr::workflow::{ActivityContext, WorkflowContext, WorkflowContextExt, WorkflowError};
use serde::{Deserialize, Serialize};

use crate::turn::{self, RunTurnInput, RunTurnOutput};

/// Register this under [`WORKFLOW_NAME`] via `Registry::add_workflow`.
pub const WORKFLOW_NAME: &str = "GooseAgentExecution";
/// Register this under [`ACTIVITY_NAME`] via `Registry::add_activity`.
pub const ACTIVITY_NAME: &str = "run_goose_turn";

/// Input for a full (possibly multi-turn) Goose agent execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GooseWorkflowInput {
    /// Program to spawn, e.g. `"goose"`.
    pub program: String,
    /// Arguments, e.g. `["acp", "--with-builtin", "developer"]`.
    pub args: Vec<String>,
    /// Extra environment variables for the spawned process.
    pub env: Vec<(String, String)>,
    /// Working directory handed to Goose.
    pub cwd: PathBuf,
    /// One user message per turn, sent in order.
    pub tasks: Vec<String>,
}

/// Full record of a completed (possibly multi-turn) execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GooseWorkflowOutput {
    /// The Goose session these turns ran in — reusable for a later,
    /// separately scheduled workflow that continues the same conversation.
    pub session_id: String,
    /// One entry per turn, in order.
    pub turns: Vec<RunTurnOutput>,
    /// The last turn's response text.
    pub final_response: String,
}

/// Orchestrator: run `input.tasks` as sequential turns in the same session.
pub async fn goose_workflow(ctx: WorkflowContext) -> dapr::workflow::Result<Option<String>> {
    let input: GooseWorkflowInput = ctx.get_input_typed()?;

    let mut session_id: Option<String> = None;
    let mut turns: Vec<RunTurnOutput> = Vec::with_capacity(input.tasks.len());

    for task in &input.tasks {
        let turn_input = RunTurnInput {
            program: input.program.clone(),
            args: input.args.clone(),
            env: input.env.clone(),
            cwd: input.cwd.clone(),
            session_id: session_id.clone(),
            task: task.clone(),
        };

        let turn_output: RunTurnOutput = ctx.call_activity_typed(ACTIVITY_NAME, turn_input).await?;
        session_id = Some(turn_output.session_id.clone());
        turns.push(turn_output);
    }

    let final_response = turns
        .last()
        .map(|t| t.response_text.clone())
        .unwrap_or_default();
    let output = GooseWorkflowOutput {
        session_id: session_id.unwrap_or_default(),
        turns,
        final_response,
    };

    Ok(Some(serde_json::to_string(&output)?))
}

/// Activity: run exactly one turn against a freshly spawned Goose process.
pub async fn run_goose_turn_activity(
    _ctx: ActivityContext,
    input: Option<String>,
) -> dapr::workflow::Result<Option<String>> {
    let input: RunTurnInput = match input {
        Some(raw) => serde_json::from_str(&raw)?,
        None => return Err(WorkflowError::Other("missing turn input".to_string())),
    };
    log::info!(
        "running goose turn: task={:?} resuming_session={:?}",
        input.task,
        input.session_id
    );

    let output = turn::run_turn(input)
        .await
        .map_err(|e| WorkflowError::Other(e.to_string()))?;

    Ok(Some(serde_json::to_string(&output)?))
}
