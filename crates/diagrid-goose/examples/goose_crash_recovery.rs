//! Crash recovery test against a **real** `goose acp` process.
//!
//! Two turns run in the same Goose session. An embedded, in-process
//! OpenAI-compatible stub server stands in for a real model provider so
//! this needs no API key and no network access — but Goose itself is the
//! real, unmodified binary, actually spawned, actually speaking ACP.
//!
//! On run 1 the stub stalls turn 2's model call. It recognizes that call by
//! its content, not by counting requests: Goose also fires a background
//! session-title request during the first turn, so "the second request" is
//! as likely to be turn 1's reply as anything else. Goose's own process is
//! blocked on that call while turn 2's activity is in flight and turn 1 is
//! already recorded in workflow history — so a real `kill -TERM <pid>` at
//! that point interrupts a genuine multi-process exchange (this wrapper ->
//! goose acp -> stub server) on a turn that has to be resumed through
//! `session/load`, not merely restarted.
//!
//! Run 2 checks what actually happened and exits non-zero if any of it is
//! wrong:
//! - turn 1's model call ran exactly once across both runs (a completed turn
//!   is replayed from workflow history, never re-executed);
//! - turn 2's model call ran twice: interrupted on run 1, retried on run 2;
//! - the retried call carried turn 1's exchange (a fresh `goose acp` process
//!   restored the conversation from Goose's own session store) and turn 2's
//!   prompt exactly once, followed by the crate's resume prompt — Goose had
//!   already saved turn 2's prompt when run 1 died, so it must not be sent
//!   again;
//! - both turns ran in one Goose session and returned their own replies.
//!
//! Usage (from `crates/diagrid-goose`):
//! ```bash
//! rm -rf /tmp/diagrid_goose_e2e_*
//!
//! # Run 1 (stalls in turn 2 — SIGTERM the printed pid from another shell):
//! GOOSE_BIN=/path/to/goose dapr run --app-id goose-crash-test \
//!     --resources-path ./examples/resources \
//!     -- cargo run --example goose_crash_recovery
//!
//! # Run 2 (resumes turn 2 in the same Goose session, then verifies):
//! GOOSE_BIN=/path/to/goose dapr run --app-id goose-crash-test \
//!     --resources-path ./examples/resources \
//!     -- cargo run --example goose_crash_recovery
//! ```

use std::fs;
use std::path::Path;
use std::process;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::State;
use axum::http::header;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use dapr::workflow::{
    FetchOptions, RuntimeStatus, ScheduleOptions, WorkflowClient, WorkflowMetadata,
};
use diagrid_goose::turn::RESUME_PROMPT;
use diagrid_goose::{GooseWorkflowInput, GooseWorkflowOutput};

const STATE_FILE: &str = "/tmp/diagrid_goose_e2e_state.json";
/// Goose's own config and session store for this test (`GOOSE_PATH_ROOT`),
/// kept apart from the real `~/.local/share/goose`.
const GOOSE_ROOT: &str = "/tmp/diagrid_goose_e2e_goose";
const STALL_SECS: u64 = 60;

/// `(prompt, the stub's reply to it)`, one entry per turn.
const TURNS: [(&str, &str); 2] = [
    ("Say 'turn one done' and nothing else.", "turn one done"),
    ("Say 'turn two done' and nothing else.", "turn two done"),
];
/// The turn (0-based) whose model call stalls on run 1.
const CRASH_TURN: usize = 1;

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
struct TestState {
    run_count: i32,
    instance_id: Option<String>,
    /// Every turn's model call the stub received, across all runs.
    model_calls: Vec<ModelCall>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ModelCall {
    run: i32,
    turn: usize,
    /// The conversation as the model saw it, reduced to `user: <turn prompt>`
    /// and `assistant: <reply>` entries in order (system/context text dropped).
    history: Vec<String>,
}

/// Serializes load-modify-save of the state file: the stub (on axum's tasks)
/// and `main` both update it.
static STATE_LOCK: Mutex<()> = Mutex::new(());

fn load_state() -> TestState {
    if Path::new(STATE_FILE).exists() {
        serde_json::from_str(&fs::read_to_string(STATE_FILE).unwrap_or_default())
            .unwrap_or_default()
    } else {
        TestState::default()
    }
}

fn update_state<R>(f: impl FnOnce(&mut TestState) -> R) -> R {
    let _guard = STATE_LOCK.lock().unwrap();
    let mut state = load_state();
    let result = f(&mut state);
    fs::write(STATE_FILE, serde_json::to_string_pretty(&state).unwrap()).unwrap();
    result
}

/// Text of an OpenAI chat message's `content`, whether it is a plain string
/// or an array of `{"type": "text", "text": ...}` parts.
fn content_text(message: &Value) -> String {
    match &message["content"] {
        Value::String(text) => text.clone(),
        Value::Array(parts) => parts
            .iter()
            .filter_map(|part| part["text"].as_str())
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

/// Goose's background session-naming call, as opposed to a turn's reply.
fn is_title_request(request: &Value) -> bool {
    request["messages"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|m| m["role"] == "system" && content_text(m).contains("Generate a short title"))
}

/// Which turn `request` is the model call for (the last turn prompt in it),
/// plus the conversation it carries — see [`ModelCall::history`].
fn summarize(request: &Value) -> (Option<usize>, Vec<String>) {
    let mut turn = None;
    let mut history = Vec::new();
    for message in request["messages"].as_array().into_iter().flatten() {
        let text = content_text(message);
        match message["role"].as_str() {
            Some("user") => {
                for (i, (prompt, _)) in TURNS.iter().enumerate() {
                    for _ in text.matches(prompt) {
                        history.push(format!("user: {prompt}"));
                        turn = Some(i);
                    }
                }
                for _ in text.matches(RESUME_PROMPT) {
                    history.push(format!("user: {RESUME_PROMPT}"));
                }
            }
            Some("assistant") => history.push(format!("assistant: {}", text.trim())),
            _ => {}
        }
    }
    (turn, history)
}

/// A complete streamed (SSE) chat completion — goose's openai provider
/// streams by default.
fn sse_reply(id: &str, text: &str) -> impl IntoResponse {
    let chunk = |delta: Value, finish_reason: Value| {
        json!({
            "id": id, "object": "chat.completion.chunk", "created": 0, "model": "stub",
            "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}],
        })
    };
    let body = format!(
        "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
        chunk(json!({"role": "assistant", "content": text}), Value::Null),
        chunk(json!({}), json!("stop")),
    );
    ([(header::CONTENT_TYPE, "text/event-stream")], body)
}

#[derive(Clone)]
struct Stub {
    run: i32,
    next_id: Arc<AtomicU32>,
}

/// Minimal OpenAI-compatible `/v1/chat/completions`: answers each turn with
/// that turn's canned reply, recording what the model was shown; on run 1
/// it stalls [`CRASH_TURN`]'s call long enough for an external SIGTERM.
async fn chat_completions(
    State(stub): State<Stub>,
    Json(request): Json<Value>,
) -> impl IntoResponse {
    // Unique per response, like a real provider's: goose stores it as the
    // assistant message's id.
    let id = format!(
        "chatcmpl-stub-{}-{}",
        stub.run,
        stub.next_id.fetch_add(1, Ordering::SeqCst)
    );
    if is_title_request(&request) {
        return sse_reply(&id, "Stub session");
    }

    let (turn, history) = summarize(&request);
    let Some(turn) = turn else {
        println!(">>> STUB: model call matches no turn prompt, answering generically");
        return sse_reply(&id, "Understood.");
    };
    println!(
        ">>> STUB: turn {} model call (run {}), history: {history:?}",
        turn + 1,
        stub.run
    );
    update_state(|state| {
        state.model_calls.push(ModelCall {
            run: stub.run,
            turn,
            history,
        })
    });

    if stub.run == 1 && turn == CRASH_TURN {
        println!(
            ">>> STUB: stalling turn {} for {STALL_SECS}s (pid={})",
            turn + 1,
            process::id()
        );
        println!(">>> Send: kill -TERM {}", process::id());
        tokio::time::sleep(Duration::from_secs(STALL_SECS)).await;
        println!(">>> STUB: woke up without being killed, answering anyway");
    }
    sse_reply(&id, TURNS[turn].1)
}

/// Fixed on purpose, not OS-assigned: `OPENAI_HOST` is captured once into the
/// *workflow's* durable input at schedule time (run 1) and replayed verbatim
/// on every later resumption (run 2, ...) — exactly as it should be, since
/// that input must stay valid for the life of the workflow, however many
/// times the process restarts. A real provider endpoint doesn't move between
/// restarts either; an OS-assigned ephemeral port here would just be
/// simulating an outage that has nothing to do with what this test verifies.
const STUB_PORT: u16 = 48173;

async fn start_stub_server(run: i32) -> anyhow::Result<u16> {
    let stub = Stub {
        run,
        next_id: Arc::new(AtomicU32::new(0)),
    };
    let app = Router::new()
        .route("/v1/chat/completions", post(chat_completions))
        .with_state(stub);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", STUB_PORT)).await?;
    let port = listener.local_addr()?.port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    Ok(port)
}

/// Everything run 2 must be able to show; `Err` says what went wrong.
fn verify(metadata: &WorkflowMetadata, state: &TestState) -> Result<(), String> {
    if metadata.runtime_status != RuntimeStatus::Completed {
        return Err(format!(
            "workflow ended {:?}: {:?}",
            metadata.runtime_status, metadata.failure_details
        ));
    }
    let output: GooseWorkflowOutput =
        serde_json::from_str(metadata.serialized_output.as_deref().unwrap_or_default())
            .map_err(|e| format!("unreadable workflow output: {e}"))?;

    if output.turns.len() != TURNS.len() {
        return Err(format!(
            "{} turns completed, expected {}",
            output.turns.len(),
            TURNS.len()
        ));
    }
    for (i, (turn, (_, reply))) in output.turns.iter().zip(TURNS).enumerate() {
        if turn.response_text.trim() != reply
            || !diagrid_goose::turn::is_end_turn(&turn.stop_reason)
        {
            return Err(format!(
                "turn {} returned {turn:?}, expected {reply:?}",
                i + 1
            ));
        }
        if turn.session_id != output.session_id {
            return Err(format!(
                "turn {} ran in Goose session {}, not {}",
                i + 1,
                turn.session_id,
                output.session_id
            ));
        }
    }

    let calls = |turn: usize| -> Vec<&ModelCall> {
        state
            .model_calls
            .iter()
            .filter(|c| c.turn == turn)
            .collect()
    };
    let first = calls(0);
    if first.len() != 1 {
        return Err(format!(
            "turn 1's model call ran {} times; a completed turn must not re-run",
            first.len()
        ));
    }
    let crashed = calls(CRASH_TURN);
    if crashed.len() != 2 || crashed[0].run != 1 || crashed[1].run == 1 {
        return Err(format!(
            "turn {} wasn't interrupted on run 1 and retried after the restart (model calls: \
             {crashed:?}) — was the worker SIGTERM'd during the stall?",
            CRASH_TURN + 1
        ));
    }
    // Turn 2's prompt was already in Goose's session when run 1 died, so the
    // retry must ask the model to finish it rather than send it again.
    let expected = vec![
        format!("user: {}", TURNS[0].0),
        format!("assistant: {}", TURNS[0].1),
        format!("user: {}", TURNS[1].0),
        format!("user: {RESUME_PROMPT}"),
    ];
    if crashed[1].history != expected {
        return Err(format!(
            "the retried turn {} showed the model {:?}, expected {expected:?}",
            CRASH_TURN + 1,
            crashed[1].history
        ));
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let run = update_state(|state| {
        state.run_count += 1;
        state.run_count
    });
    println!("============================================================");
    println!("RUN #{run} (pid={})", process::id());
    println!("============================================================");

    let stub_port = start_stub_server(run).await?;
    println!("stub OpenAI-compatible server listening on 127.0.0.1:{stub_port}");

    let goose_bin = std::env::var("GOOSE_BIN").unwrap_or_else(|_| "goose".to_string());

    let mut client = WorkflowClient::new().await?;
    diagrid_goose::register(client.registry_mut());
    let worker = client.start_worker().await?;
    println!("worker started");

    let instance_id = match load_state().instance_id {
        Some(instance_id) => {
            println!("workflow already scheduled, polling existing instance {instance_id}");
            instance_id
        }
        None => {
            let instance_id = format!(
                "goose-crash-recovery-e2e-{}",
                SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs()
            );
            let input = GooseWorkflowInput {
                program: goose_bin,
                args: vec!["acp".to_string()],
                env: vec![
                    ("GOOSE_PATH_ROOT".to_string(), GOOSE_ROOT.to_string()),
                    ("GOOSE_DISABLE_KEYRING".to_string(), "1".to_string()),
                    ("GOOSE_PROVIDER".to_string(), "openai".to_string()),
                    ("GOOSE_MODEL".to_string(), "stub-model".to_string()),
                    (
                        "OPENAI_HOST".to_string(),
                        format!("http://127.0.0.1:{stub_port}"),
                    ),
                    ("OPENAI_API_KEY".to_string(), "stub-key".to_string()),
                ],
                cwd: std::env::temp_dir(),
                tasks: TURNS.iter().map(|(prompt, _)| prompt.to_string()).collect(),
            };

            println!("scheduling new workflow instance {instance_id}");
            client
                .schedule_workflow(
                    diagrid_goose::WORKFLOW_NAME,
                    ScheduleOptions::new()
                        .with_instance_id(&instance_id)
                        .try_with_input(input)?,
                )
                .await?;
            update_state(|state| state.instance_id = Some(instance_id.clone()));
            instance_id
        }
    };

    let metadata = client
        .wait_for_workflow_completion_with_options(
            &instance_id,
            FetchOptions::new().with_fetch_payloads(true),
            Some(Duration::from_secs(120)),
        )
        .await?;

    println!("============================================================");
    println!("workflow status: {:?}", metadata.runtime_status);
    if let Some(output) = &metadata.serialized_output {
        if let Ok(output) = serde_json::from_str::<GooseWorkflowOutput>(output) {
            println!("session_id: {}", output.session_id);
            for (i, turn) in output.turns.iter().enumerate() {
                println!(
                    "  turn {}: stop_reason={} response={:?}",
                    i + 1,
                    turn.stop_reason,
                    turn.response_text
                );
            }
        }
    }
    let verdict = verify(&metadata, &load_state());
    worker.shutdown().await?;
    println!("worker shutdown");

    match verdict {
        Ok(()) => {
            println!(
                ">>> TEST PASSED: real Goose turn interrupted by SIGTERM resumed via session/load"
            );
            println!("============================================================");
            Ok(())
        }
        Err(reason) => {
            println!(">>> TEST FAILED: {reason}");
            println!("============================================================");
            process::exit(1);
        }
    }
}
