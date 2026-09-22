//! Runs exactly one turn of a Goose conversation against a freshly spawned
//! `goose acp` process, over Goose's stable, already-published ACP interface.
//!
//! Nothing here depends on Goose's source or its internal crates (`goose`,
//! `goose-agent`, ...) — this crate only ever links against the standalone
//! `agent-client-protocol` crate and spawns whatever `goose` binary the
//! caller points it at. Goose's own agent loop, tool calls, and extensions
//! all run unmodified, inside the spawned process; the whole exchange from
//! this side is: initialize, resume-or-create a session, send one prompt,
//! collect the response.
//!
//! Session continuity across turns rides Goose's own `session/load` support
//! (it advertises `loadSession: true` on `Initialize`): the caller threads
//! the `session_id` returned by one call into the next one's input, and a
//! brand new process reconstructs the same conversation from Goose's own
//! on-disk session history. That's what makes it safe for each turn to be
//! its own independent, freshly spawned Dapr activity — there is no shared
//! process or connection state that a crash could strand.

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use agent_client_protocol::schema::v1::{
    ContentBlock, InitializeRequest, LoadSessionRequest, NewSessionRequest, PromptRequest,
    RequestPermissionOutcome, RequestPermissionRequest, RequestPermissionResponse,
    SelectedPermissionOutcome, SessionId, SessionNotification, SessionUpdate, StopReason,
    TextContent,
};
use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::{AcpAgent, AcpAgentConfig, Agent, ConnectionTo};
use serde::{Deserialize, Serialize};

/// Everything one turn needs: how to spawn Goose, which conversation to
/// continue (if any), and what to say.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunTurnInput {
    /// Program to spawn, e.g. `"goose"`. Resolved via PATH like any command.
    pub program: String,
    /// Arguments, e.g. `["acp", "--with-builtin", "developer"]`.
    pub args: Vec<String>,
    /// Extra environment variables for the spawned process (provider
    /// selection, API keys/endpoints, etc.) layered on top of the normally
    /// inherited environment — PATH and friends still resolve.
    pub env: Vec<(String, String)>,
    /// Working directory handed to Goose for this session.
    pub cwd: PathBuf,
    /// `None` starts a new session; `Some(id)` resumes one via `session/load`.
    pub session_id: Option<String>,
    /// The user message for this turn.
    pub task: String,
}

/// What one turn produced.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunTurnOutput {
    /// Carry this into the next turn's [`RunTurnInput::session_id`].
    pub session_id: String,
    /// Concatenated text of the agent's reply for this turn.
    pub response_text: String,
    /// Debug-formatted `StopReason` (`EndTurn`, `MaxTokens`, ...).
    pub stop_reason: String,
}

/// Spawn Goose, run one turn to completion, and shut the process down.
pub async fn run_turn(input: RunTurnInput) -> anyhow::Result<RunTurnOutput> {
    let mut config = AcpAgentConfig::new(PathBuf::from_str(&input.program)?);
    for arg in &input.args {
        config = config.arg(arg.clone());
    }
    config = config.envs(input.env.iter().cloned());
    let agent = AcpAgent::new(config);

    // Accumulates AgentMessageChunk text for the *current* prompt. Cleared
    // right before sending a prompt so history replayed by session/load
    // (Goose replays prior turns as message chunks when resuming) doesn't
    // leak into this turn's result — mirrors Goose's own ACP client code
    // (crates/goose-cli/src/commands/roam_client.rs) doing the same thing.
    let sink: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));
    let notify_sink = sink.clone();
    let clear_sink = sink.clone();
    let read_sink = sink.clone();

    let cwd = input.cwd.clone();
    let session_id_in = input.session_id.clone();
    let task = input.task.clone();

    let (session_id, stop_reason) = agent_client_protocol::Client
        .builder()
        .on_receive_notification(
            async move |notification: SessionNotification, _cx| {
                if let SessionUpdate::AgentMessageChunk(chunk) = &notification.update {
                    if let ContentBlock::Text(text) = &chunk.content {
                        notify_sink.lock().unwrap().push_str(&text.text);
                    }
                }
                Ok(())
            },
            agent_client_protocol::on_receive_notification!(),
        )
        .on_receive_request(
            async move |request: RequestPermissionRequest, responder, _connection| {
                // Auto-approve: this wrapper runs Goose non-interactively (no
                // human in the loop to ask), the same policy the SDK's own
                // "yolo" reference client uses. A production deployment that
                // needs human approval would surface this as a Dapr external
                // event instead of answering it inline.
                let option_id = request.options.first().map(|opt| opt.option_id.clone());
                let outcome = match option_id {
                    Some(id) => {
                        RequestPermissionOutcome::Selected(SelectedPermissionOutcome::new(id))
                    }
                    None => RequestPermissionOutcome::Cancelled,
                };
                responder.respond(RequestPermissionResponse::new(outcome))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .connect_with(agent, move |cx: ConnectionTo<Agent>| async move {
            cx.send_request(InitializeRequest::new(ProtocolVersion::V1))
                .block_task()
                .await?;

            let session_id = match session_id_in {
                Some(id) => {
                    let session_id = SessionId::from(id);
                    cx.send_request(LoadSessionRequest::new(session_id.clone(), cwd.clone()))
                        .block_task()
                        .await?;
                    // Discard the replayed history now that Goose has
                    // reconstructed its side of the conversation — only
                    // this turn's own reply should end up in the sink.
                    clear_sink.lock().unwrap().clear();
                    session_id
                }
                None => {
                    cx.send_request(NewSessionRequest::new(cwd.clone()))
                        .block_task()
                        .await?
                        .session_id
                }
            };

            let prompt_response = cx
                .send_request(PromptRequest::new(
                    session_id.clone(),
                    vec![ContentBlock::Text(TextContent::new(task.clone()))],
                ))
                .block_task()
                .await?;

            Ok::<_, agent_client_protocol::Error>((session_id, prompt_response.stop_reason))
        })
        .await?;

    let response_text = read_sink.lock().unwrap().clone();
    Ok(RunTurnOutput {
        session_id: session_id.to_string(),
        response_text,
        stop_reason: format!("{stop_reason:?}"),
    })
}

/// Convenience for callers that only care whether the turn ended normally.
pub fn is_end_turn(stop_reason: &str) -> bool {
    stop_reason == format!("{:?}", StopReason::EndTurn)
}
