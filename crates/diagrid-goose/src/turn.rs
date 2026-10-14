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
//!
//! # Retried turns
//!
//! Goose saves a turn's prompt to its session *before* it calls the model,
//! so an attempt that dies mid-turn leaves its prompt behind. Re-sending the
//! task on the retry would show the model the same request twice — and an
//! imperative one ("append a line to notes.md") may then be carried out
//! twice. So the caller also threads [`RunTurnOutput::prompt_count`] into the
//! next turn, and a resumed turn checks it against the history `session/load`
//! replays: a prompt beyond that count is an earlier attempt of this same
//! turn. If that attempt's reply finished, it is returned as is; otherwise
//! [`RESUME_PROMPT`] is sent instead of the task, so the model finishes the
//! interrupted request rather than receiving it again.

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use agent_client_protocol::schema::v1::{
    ContentBlock, InitializeRequest, LoadSessionRequest, MessageId, NewSessionRequest,
    PromptRequest, RequestPermissionOutcome, RequestPermissionRequest, RequestPermissionResponse,
    SelectedPermissionOutcome, SessionId, SessionNotification, SessionUpdate, StopReason,
    TextContent,
};
use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::{AcpAgent, AcpAgentConfig, Agent, ConnectionTo};
use serde::{Deserialize, Serialize};

/// Sent in place of the task when an earlier attempt of this turn already
/// delivered it to Goose but died before the reply finished.
pub const RESUME_PROMPT: &str = "Your reply to my last message was interrupted before it \
     finished. Pick up where it stopped and finish it, without redoing steps that already \
     completed.";

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
    /// How many prompts that session held when its last turn completed: the
    /// previous turn's [`RunTurnOutput::prompt_count`] (`0` for a new session).
    pub prompt_count: usize,
    /// The user message for this turn.
    pub task: String,
}

/// What one turn produced.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunTurnOutput {
    /// Carry this into the next turn's [`RunTurnInput::session_id`].
    pub session_id: String,
    /// Carry this into the next turn's [`RunTurnInput::prompt_count`].
    pub prompt_count: usize,
    /// Concatenated text of the agent's reply for this turn.
    pub response_text: String,
    /// Debug-formatted `StopReason` (`EndTurn`, `MaxTokens`, ...).
    pub stop_reason: String,
}

/// One user-visible piece of session history, as `session/update`
/// notifications deliver it (consecutive chunks of a message merged).
#[derive(Debug, Clone, PartialEq)]
enum Entry {
    Prompt { id: Option<MessageId>, text: String },
    Reply(String),
    Tool,
}

fn record(entries: &mut Vec<Entry>, update: &SessionUpdate) {
    match update {
        SessionUpdate::UserMessageChunk(chunk) => {
            let text = match &chunk.content {
                ContentBlock::Text(text) => text.text.as_str(),
                _ => "",
            };
            match entries.last_mut() {
                Some(Entry::Prompt { id, text: buf })
                    if id.is_some() && *id == chunk.message_id =>
                {
                    buf.push_str(text)
                }
                _ => entries.push(Entry::Prompt {
                    id: chunk.message_id.clone(),
                    text: text.to_string(),
                }),
            }
        }
        SessionUpdate::AgentMessageChunk(chunk) => {
            if let ContentBlock::Text(text) = &chunk.content {
                match entries.last_mut() {
                    Some(Entry::Reply(buf)) => buf.push_str(&text.text),
                    _ => entries.push(Entry::Reply(text.text.clone())),
                }
            }
        }
        SessionUpdate::ToolCall(_) | SessionUpdate::ToolCallUpdate(_) => entries.push(Entry::Tool),
        _ => {}
    }
}

fn reply_text(entries: &[Entry]) -> String {
    entries
        .iter()
        .filter_map(|entry| match entry {
            Entry::Reply(text) => Some(text.as_str()),
            _ => None,
        })
        .collect()
}

fn prompts(entries: &[Entry]) -> usize {
    entries
        .iter()
        .filter(|entry| matches!(entry, Entry::Prompt { .. }))
        .count()
}

/// What a resumed session's history says about this turn.
#[derive(Debug, PartialEq)]
enum Resume {
    /// No earlier attempt of this turn got its prompt into the session.
    Fresh,
    /// An earlier attempt's prompt is in the session but its reply never
    /// finished; `partial` is whatever reply text that attempt got out.
    Interrupted { partial: String },
    /// An earlier attempt finished before its result was recorded.
    Finished { reply: String },
}

/// Classify `history` (as replayed by `session/load`) for a turn sending
/// `task`, given the session's `prompt_count` when its last turn completed.
fn classify(history: &[Entry], prompt_count: usize, task: &str) -> Result<Resume, String> {
    let held = prompts(history);
    if held < prompt_count {
        return Err(format!(
            "Goose session holds {held} prompts, but {prompt_count} were recorded when its last \
             turn completed — this isn't the history the workflow left behind"
        ));
    }
    let Some(start) = history
        .iter()
        .enumerate()
        .filter(|(_, entry)| matches!(entry, Entry::Prompt { .. }))
        .nth(prompt_count)
        .map(|(i, _)| i)
    else {
        return Ok(Resume::Fresh);
    };
    match &history[start] {
        Entry::Prompt { text, .. } if text.trim() == task.trim() => {}
        other => {
            return Err(format!(
                "Goose session gained an unexpected message after its last completed turn: \
                 {other:?}"
            ))
        }
    }

    // Goose's agent loop only ends a turn on a reply with no tool call after
    // it, so a trailing reply means the earlier attempt got all the way through.
    let tail = &history[start + 1..];
    Ok(match tail.last() {
        Some(Entry::Reply(_)) => Resume::Finished {
            reply: reply_text(tail),
        },
        _ => Resume::Interrupted {
            partial: reply_text(tail),
        },
    })
}

/// Spawn Goose, run one turn to completion, and shut the process down.
pub async fn run_turn(input: RunTurnInput) -> anyhow::Result<RunTurnOutput> {
    let mut config = AcpAgentConfig::new(PathBuf::from_str(&input.program)?);
    for arg in &input.args {
        config = config.arg(arg.clone());
    }
    config = config.envs(input.env.iter().cloned());
    let agent = AcpAgent::new(config);

    // What Goose streams back: on session/load, the session's history
    // replayed; during session/prompt, this turn's reply.
    let updates: Arc<Mutex<Vec<Entry>>> = Arc::new(Mutex::new(Vec::new()));
    let notify_updates = updates.clone();
    let history_updates = updates.clone();

    let cwd = input.cwd.clone();
    let session_id_in = input.session_id.clone();
    let prompt_count_in = input.prompt_count;
    let task = input.task.clone();

    let (session_id, prompt_count, resume, stop_reason) = agent_client_protocol::Client
        .builder()
        .on_receive_notification(
            async move |notification: SessionNotification, _cx| {
                record(&mut notify_updates.lock().unwrap(), &notification.update);
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

            let (session_id, prompt_count, resume) = match session_id_in {
                Some(id) => {
                    let session_id = SessionId::from(id);
                    cx.send_request(LoadSessionRequest::new(session_id.clone(), cwd.clone()))
                        .block_task()
                        .await?;
                    // Goose replays the whole history before answering
                    // session/load, and notifications are handled in order,
                    // so `updates` holds exactly that history now. Take it,
                    // leaving only this turn's own reply to accumulate.
                    let history = std::mem::take(&mut *history_updates.lock().unwrap());
                    let resume = classify(&history, prompt_count_in, &task)
                        .map_err(|e| agent_client_protocol::Error::internal_error().data(e))?;
                    (session_id, prompts(&history), resume)
                }
                None => {
                    let session_id = cx
                        .send_request(NewSessionRequest::new(cwd.clone()))
                        .block_task()
                        .await?
                        .session_id;
                    (session_id, 0, Resume::Fresh)
                }
            };

            let prompt = match &resume {
                Resume::Fresh => task.clone(),
                Resume::Interrupted { .. } => {
                    log::warn!(
                        "session {session_id}: an earlier attempt of this turn was interrupted; \
                         resuming it"
                    );
                    RESUME_PROMPT.to_string()
                }
                Resume::Finished { .. } => {
                    log::warn!(
                        "session {session_id}: an earlier attempt of this turn already finished; \
                         reusing its reply"
                    );
                    return Ok((session_id, prompt_count, resume, StopReason::EndTurn));
                }
            };
            let prompt_response = cx
                .send_request(PromptRequest::new(
                    session_id.clone(),
                    vec![ContentBlock::Text(TextContent::new(prompt))],
                ))
                .block_task()
                .await?;

            Ok::<_, agent_client_protocol::Error>((
                session_id,
                prompt_count + 1,
                resume,
                prompt_response.stop_reason,
            ))
        })
        .await?;

    let reply = reply_text(&updates.lock().unwrap());
    let response_text = match resume {
        Resume::Fresh => reply,
        Resume::Interrupted { partial } => partial + &reply,
        Resume::Finished { reply } => reply,
    };
    Ok(RunTurnOutput {
        session_id: session_id.to_string(),
        prompt_count,
        response_text,
        stop_reason: format!("{stop_reason:?}"),
    })
}

/// Convenience for callers that only care whether the turn ended normally.
pub fn is_end_turn(stop_reason: &str) -> bool {
    stop_reason == format!("{:?}", StopReason::EndTurn)
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_client_protocol::schema::v1::ContentChunk;

    fn prompt(text: &str) -> Entry {
        Entry::Prompt {
            id: None,
            text: text.to_string(),
        }
    }

    fn reply(text: &str) -> Entry {
        Entry::Reply(text.to_string())
    }

    fn user_chunk(id: Option<&str>, text: &str) -> SessionUpdate {
        let mut chunk = ContentChunk::new(ContentBlock::Text(TextContent::new(text)));
        if let Some(id) = id {
            chunk = chunk.message_id(MessageId::new(id.to_string()));
        }
        SessionUpdate::UserMessageChunk(chunk)
    }

    fn agent_chunk(text: &str) -> SessionUpdate {
        SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(TextContent::new(
            text,
        ))))
    }

    #[test]
    fn record_splits_prompts_by_message_id() {
        let mut entries = Vec::new();
        for update in [
            user_chunk(Some("m1"), "turn "),
            user_chunk(Some("m1"), "one"),
            agent_chunk("done "),
            agent_chunk("one"),
            user_chunk(Some("m2"), "turn two"),
            user_chunk(Some("m3"), "turn two"),
            user_chunk(None, "a"),
            user_chunk(None, "b"),
        ] {
            record(&mut entries, &update);
        }
        let texts: Vec<_> = entries
            .iter()
            .map(|entry| match entry {
                Entry::Prompt { text, .. } => format!("P:{text}"),
                Entry::Reply(text) => format!("R:{text}"),
                Entry::Tool => "T".to_string(),
            })
            .collect();
        assert_eq!(
            texts,
            [
                "P:turn one",
                "R:done one",
                "P:turn two",
                "P:turn two",
                "P:a",
                "P:b"
            ]
        );
    }

    #[test]
    fn first_attempt_is_fresh() {
        let history = [prompt("one"), reply("done one")];
        assert_eq!(classify(&history, 1, "two"), Ok(Resume::Fresh));
    }

    #[test]
    fn repeated_task_text_is_still_fresh() {
        // The previous turn sent the very same text: only the count can tell
        // it apart from an earlier attempt of this turn.
        let history = [prompt("continue"), reply("ok")];
        assert_eq!(classify(&history, 1, "continue"), Ok(Resume::Fresh));
    }

    #[test]
    fn prompt_without_reply_is_interrupted() {
        let history = [prompt("one"), reply("done one"), prompt("two")];
        assert_eq!(
            classify(&history, 1, "two"),
            Ok(Resume::Interrupted {
                partial: String::new()
            })
        );
    }

    #[test]
    fn crash_mid_tool_use_is_interrupted() {
        let history = [
            prompt("one"),
            reply("done one"),
            prompt("two"),
            reply("checking. "),
            Entry::Tool,
            Entry::Tool,
        ];
        assert_eq!(
            classify(&history, 1, "two"),
            Ok(Resume::Interrupted {
                partial: "checking. ".to_string()
            })
        );
    }

    #[test]
    fn repeated_interruptions_stay_interrupted() {
        let history = [
            prompt("one"),
            reply("done one"),
            prompt("two"),
            prompt(RESUME_PROMPT),
        ];
        assert_eq!(
            classify(&history, 1, "two"),
            Ok(Resume::Interrupted {
                partial: String::new()
            })
        );
    }

    #[test]
    fn finished_but_unrecorded_attempt_is_reused() {
        let history = [
            prompt("one"),
            reply("done one"),
            prompt("two"),
            reply("checking. "),
            Entry::Tool,
            reply("done two"),
        ];
        assert_eq!(
            classify(&history, 1, "two"),
            Ok(Resume::Finished {
                reply: "checking. done two".to_string()
            })
        );
    }

    #[test]
    fn lost_history_is_an_error() {
        let history = [prompt("one"), reply("done one")];
        assert!(classify(&history, 2, "three").is_err());
    }

    #[test]
    fn foreign_message_is_an_error() {
        let history = [prompt("one"), reply("done one"), prompt("something else")];
        assert!(classify(&history, 1, "two").is_err());
    }
}
