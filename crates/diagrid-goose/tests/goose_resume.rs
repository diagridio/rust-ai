//! `run_turn` against a real `goose acp` binary, retrying a turn after an
//! attempt that died mid-turn and after one that finished but whose result
//! never made it back. No Dapr needed: the "crash" is dropping the turn's
//! future, which kills its `goose acp` process group just as a dying worker
//! takes it down, and a retry is simply the same `RunTurnInput` again.
//!
//! These pin down the parts of Goose's behavior the retry logic relies on
//! (what `session/load` replays, and that a prompt is saved before the model
//! call), so re-run them whenever the Goose version changes:
//! ```bash
//! GOOSE_BIN=/path/to/goose cargo test -p diagrid-goose -- --ignored
//! ```

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::http::header;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{json, Value};
use tokio::sync::Notify;

use diagrid_goose::turn::{run_turn, RunTurnInput, RESUME_PROMPT};

const FIRST: &str = "Say 'one done' and nothing else.";
const SECOND: &str = "Say 'two done' and nothing else.";

/// OpenAI-compatible stand-in for a model provider.
#[derive(Default)]
struct Stub {
    /// What the model was shown on each turn's call: the prompts above (and
    /// [`RESUME_PROMPT`]) and assistant replies, in order.
    calls: Mutex<Vec<Vec<String>>>,
    /// While set, turn calls never answer.
    stall: AtomicBool,
    stalled: Notify,
    next_id: AtomicU32,
}

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

async fn chat_completions(
    State(stub): State<Arc<Stub>>,
    Json(request): Json<Value>,
) -> impl IntoResponse {
    let messages = request["messages"].as_array().cloned().unwrap_or_default();
    let is_title = messages
        .iter()
        .any(|m| m["role"] == "system" && content_text(m).contains("Generate a short title"));

    let mut seen = Vec::new();
    if !is_title {
        for message in &messages {
            let text = content_text(message);
            match message["role"].as_str() {
                Some("user") => {
                    for known in [FIRST, SECOND, RESUME_PROMPT] {
                        for _ in text.matches(known) {
                            seen.push(known.to_string());
                        }
                    }
                }
                Some("assistant") => seen.push(text.trim().to_string()),
                _ => {}
            }
        }
        stub.calls.lock().unwrap().push(seen.clone());
        if stub.stall.load(Ordering::SeqCst) {
            stub.stalled.notify_one();
            std::future::pending::<()>().await;
        }
    }

    let last_prompt = seen.iter().rev().find(|s| *s == FIRST || *s == SECOND);
    let reply = match last_prompt.map(String::as_str) {
        Some(FIRST) => "one done",
        Some(SECOND) => "two done",
        _ => "Test session",
    };
    let id = format!("chatcmpl-{}", stub.next_id.fetch_add(1, Ordering::SeqCst));
    let chunk = |delta: Value, finish_reason: Value| {
        json!({
            "id": id, "object": "chat.completion.chunk", "created": 0, "model": "stub",
            "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}],
        })
    };
    let body = format!(
        "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
        chunk(json!({"role": "assistant", "content": reply}), Value::Null),
        chunk(json!({}), json!("stop")),
    );
    ([(header::CONTENT_TYPE, "text/event-stream")], body)
}

async fn start_stub() -> (Arc<Stub>, String) {
    let stub = Arc::new(Stub::default());
    let app = Router::new()
        .route("/v1/chat/completions", post(chat_completions))
        .with_state(stub.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let host = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    (stub, host)
}

/// A fresh `GOOSE_PATH_ROOT`, so tests never touch the real Goose store.
fn goose_root(test: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!("diagrid-goose-{test}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    root
}

fn turn(
    host: &str,
    root: &Path,
    session_id: Option<&str>,
    prompt_count: usize,
    task: &str,
) -> RunTurnInput {
    let env = [
        ("GOOSE_PATH_ROOT", root.to_str().unwrap()),
        ("GOOSE_DISABLE_KEYRING", "1"),
        ("GOOSE_PROVIDER", "openai"),
        ("GOOSE_MODEL", "stub-model"),
        ("OPENAI_HOST", host),
        ("OPENAI_API_KEY", "stub-key"),
    ];
    RunTurnInput {
        program: std::env::var("GOOSE_BIN").expect("set GOOSE_BIN to a goose binary"),
        args: vec!["acp".to_string()],
        env: env
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        cwd: std::env::temp_dir(),
        session_id: session_id.map(str::to_string),
        prompt_count,
        task: task.to_string(),
    }
}

#[tokio::test]
#[ignore = "needs a goose binary: GOOSE_BIN=/path/to/goose cargo test -- --ignored"]
async fn interrupted_turn_resumes_without_resending_its_prompt() {
    let (stub, host) = start_stub().await;
    let root = goose_root("interrupted");

    let first = run_turn(turn(&host, &root, None, 0, FIRST)).await.unwrap();
    assert_eq!(first.response_text, "one done");

    // Die while the model call is in flight, after Goose saved the prompt.
    stub.stall.store(true, Ordering::SeqCst);
    let attempt = run_turn(turn(
        &host,
        &root,
        Some(&first.session_id),
        first.prompt_count,
        SECOND,
    ));
    tokio::select! {
        result = attempt => panic!("stalled turn returned {result:?}"),
        _ = stub.stalled.notified() => {}
    }
    stub.stall.store(false, Ordering::SeqCst);

    let retry = run_turn(turn(
        &host,
        &root,
        Some(&first.session_id),
        first.prompt_count,
        SECOND,
    ))
    .await
    .unwrap();
    assert_eq!(retry.session_id, first.session_id);
    assert_eq!(retry.response_text, "two done");
    assert_eq!(
        stub.calls.lock().unwrap().last().unwrap(),
        &[FIRST, "one done", SECOND, RESUME_PROMPT]
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
#[ignore = "needs a goose binary: GOOSE_BIN=/path/to/goose cargo test -- --ignored"]
async fn finished_turn_whose_result_was_lost_is_not_rerun() {
    let (stub, host) = start_stub().await;
    let root = goose_root("finished");

    let first = run_turn(turn(&host, &root, None, 0, FIRST)).await.unwrap();
    let second = run_turn(turn(
        &host,
        &root,
        Some(&first.session_id),
        first.prompt_count,
        SECOND,
    ))
    .await
    .unwrap();
    assert_eq!(second.response_text, "two done");

    // Deliver turn 2 again, as if its result had never reached the workflow.
    let calls = stub.calls.lock().unwrap().len();
    let again = run_turn(turn(
        &host,
        &root,
        Some(&first.session_id),
        first.prompt_count,
        SECOND,
    ))
    .await
    .unwrap();
    assert_eq!(
        stub.calls.lock().unwrap().len(),
        calls,
        "the model was called again"
    );
    assert_eq!(again.session_id, second.session_id);
    assert_eq!(again.response_text, second.response_text);
    assert_eq!(again.prompt_count, second.prompt_count);
    let _ = std::fs::remove_dir_all(&root);
}
