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
//! `session/load` rather than starting the conversation over, and without
//! sending its prompt a second time (see [`crate::turn`]'s "Retried turns").
//!
//! A turn that *fails* — Goose exits, the connection to it breaks, or it
//! returns an error — is retried the same way, up to [`TURN_MAX_ATTEMPTS`]
//! attempts in all, 5s and then 10s apart. The exception is Goose reporting
//! that the model provider needs authentication: retrying can't fix that,
//! and Goose saves the error as the turn's reply, which a retry would take
//! for a finished earlier attempt — so it fails the workflow straight away.

use std::path::PathBuf;
use std::time::Duration;

use agent_client_protocol::schema::v1::ErrorCode;
use dapr::workflow::{
    ActivityContext, ActivityOptions, RetryPolicy, WorkflowContext, WorkflowContextExt,
    WorkflowError,
};
use serde::{Deserialize, Serialize};

use crate::turn::{self, RunTurnInput, RunTurnOutput};

/// Register this under [`WORKFLOW_NAME`] via `Registry::add_workflow`.
pub const WORKFLOW_NAME: &str = "GooseAgentExecution";
/// Register this under [`ACTIVITY_NAME`] via `Registry::add_activity`.
pub const ACTIVITY_NAME: &str = "run_goose_turn";

/// Attempts per turn, the first one included.
pub const TURN_MAX_ATTEMPTS: u32 = 3;
/// Delay before a failed turn's first retry; it doubles for each one after.
const TURN_FIRST_RETRY_INTERVAL: Duration = Duration::from_secs(5);
/// Starts the error of a turn failure that retrying can't fix.
const NOT_RETRYABLE: &str = "not retryable: ";

fn turn_retry_policy() -> RetryPolicy {
    RetryPolicy::new(TURN_MAX_ATTEMPTS, TURN_FIRST_RETRY_INTERVAL)
        .with_backoff_coefficient(2.0)
        .with_handle(|failure| !failure.message.starts_with(NOT_RETRYABLE))
}

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
    /// The Goose session these turns ran in; `goose session export
    /// --session-id <id>` prints the conversation.
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
    let mut prompt_count = 0;
    let mut turns: Vec<RunTurnOutput> = Vec::with_capacity(input.tasks.len());

    for task in &input.tasks {
        let turn_input = RunTurnInput {
            program: input.program.clone(),
            args: input.args.clone(),
            env: input.env.clone(),
            cwd: input.cwd.clone(),
            session_id: session_id.clone(),
            prompt_count,
            task: task.clone(),
        };

        let turn_output: RunTurnOutput = ctx
            .call_activity_with_options_typed(
                ACTIVITY_NAME,
                ActivityOptions::new()
                    .try_with_input(turn_input)?
                    .with_retry_policy(turn_retry_policy()),
            )
            .await?;
        session_id = Some(turn_output.session_id.clone());
        prompt_count = turn_output.prompt_count;
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

    let output = turn::run_turn(input).await.map_err(|e| {
        let auth_required = e
            .downcast_ref::<agent_client_protocol::Error>()
            .is_some_and(|e| e.code == ErrorCode::AuthRequired);
        WorkflowError::Other(if auth_required {
            format!("{NOT_RETRYABLE}{e}")
        } else {
            e.to_string()
        })
    })?;

    Ok(Some(serde_json::to_string(&output)?))
}
