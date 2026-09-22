//! Crash recovery test against a **real** `goose acp` process.
//!
//! Two turns run in the same Goose session. An embedded, in-process
//! OpenAI-compatible stub server stands in for a real model provider so
//! this needs no API key and no network access — but Goose itself is the
//! real, unmodified binary, actually spawned, actually speaking ACP.
//!
//! The stub deliberately stalls its *second* response for ~20s. That call
//! happens inside Goose's own process, during turn 2's activity — so
//! sending a real `kill -TERM <pid>` at that point interrupts a genuine,
//! in-flight multi-process exchange (this wrapper -> goose acp -> stub
//! server), not a simulated one.
//!
//! Usage:
//! ```bash
//! rm -f /tmp/diagrid_goose_e2e_state.json
//!
//! # Run 1 (blocks ~20s into turn 2 — SIGTERM the printed pid from another shell):
//! GOOSE_BIN=/path/to/goose dapr run --app-id goose-crash-test \
//!     --resources-path ./examples/resources \
//!     -- cargo run --example goose_crash_recovery
//!
//! # Run 2 (resumes the same Goose session via session/load, completes):
//! GOOSE_BIN=/path/to/goose dapr run --app-id goose-crash-test \
//!     --resources-path ./examples/resources \
//!     -- cargo run --example goose_crash_recovery
//! ```

use std::fs;
use std::path::Path;
use std::process;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::Router;
use serde::{Deserialize, Serialize};

use dapr::workflow::{FetchOptions, RuntimeStatus, ScheduleOptions, WorkflowClient};
use diagrid_goose::GooseWorkflowInput;

const STATE_FILE: &str = "/tmp/diagrid_goose_e2e_state.json";
const INSTANCE_ID: &str = "goose-crash-recovery-e2e";
const STALL_ON_REQUEST: u32 = 2;
const STALL_SECS: u64 = 20;

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
struct TestState {
    run_count: i32,
    workflow_scheduled: bool,
}

fn load_state() -> TestState {
    if Path::new(STATE_FILE).exists() {
        serde_json::from_str(&fs::read_to_string(STATE_FILE).unwrap_or_default())
            .unwrap_or_default()
    } else {
        TestState::default()
    }
}

fn save_state(state: &TestState) {
    fs::write(STATE_FILE, serde_json::to_string_pretty(state).unwrap()).unwrap();
}

/// Minimal OpenAI-compatible `/v1/chat/completions` stub: always answers
/// "Understood." over SSE (goose's openai provider streams by default), but
/// stalls before responding to the `STALL_ON_REQUEST`-th call in this
/// process's lifetime — long enough for an external SIGTERM to land mid-turn.
async fn chat_completions(State(counter): State<Arc<AtomicU32>>) -> impl IntoResponse {
    let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
    println!(">>> STUB: received chat completion request #{n}");
    if n == STALL_ON_REQUEST {
        println!(
            ">>> STUB: stalling {STALL_SECS}s on request #{n} (pid={})",
            process::id()
        );
        println!(">>> Send: kill -TERM {}", process::id());
        tokio::time::sleep(Duration::from_secs(STALL_SECS)).await;
        println!(">>> STUB: woke up without being killed, answering request #{n} now");
    }

    let body = concat!(
        "data: {\"id\":\"stub\",\"object\":\"chat.completion.chunk\",\"created\":0,",
        "\"model\":\"stub\",\"choices\":[{\"index\":0,",
        "\"delta\":{\"role\":\"assistant\",\"content\":\"Understood.\"},",
        "\"finish_reason\":null}]}\n\n",
        "data: {\"id\":\"stub\",\"object\":\"chat.completion.chunk\",\"created\":0,",
        "\"model\":\"stub\",\"choices\":[{\"index\":0,\"delta\":{},",
        "\"finish_reason\":\"stop\"}]}\n\n",
        "data: [DONE]\n\n",
    );
    ([("content-type", "text/event-stream")], body)
}

/// Fixed on purpose, not OS-assigned: `OPENAI_HOST` is captured once into the
/// *workflow's* durable input at schedule time (run 1) and replayed verbatim
/// on every later resumption (run 2, ...) — exactly as it should be, since
/// that input must stay valid for the life of the workflow, however many
/// times the process restarts. A real provider endpoint doesn't move between
/// restarts either; an OS-assigned ephemeral port here would just be
/// simulating an outage that has nothing to do with what this test verifies.
const STUB_PORT: u16 = 48173;

async fn start_stub_server() -> anyhow::Result<u16> {
    let counter = Arc::new(AtomicU32::new(0));
    let app = Router::new()
        .route("/v1/chat/completions", post(chat_completions))
        .with_state(counter);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", STUB_PORT)).await?;
    let port = listener.local_addr()?.port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    Ok(port)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let mut state = load_state();
    state.run_count += 1;
    save_state(&state);
    println!("============================================================");
    println!("RUN #{} (pid={})", state.run_count, process::id());
    println!("workflow_scheduled={}", state.workflow_scheduled);
    println!("============================================================");

    let stub_port = start_stub_server().await?;
    println!("stub OpenAI-compatible server listening on 127.0.0.1:{stub_port}");

    let goose_bin = std::env::var("GOOSE_BIN").unwrap_or_else(|_| "goose".to_string());

    let mut client = WorkflowClient::new().await?;
    diagrid_goose::register(client.registry_mut());
    let worker = client.start_worker().await?;
    println!("worker started");

    if !state.workflow_scheduled {
        let input = GooseWorkflowInput {
            program: goose_bin,
            args: vec!["acp".to_string()],
            env: vec![
                ("GOOSE_PROVIDER".to_string(), "openai".to_string()),
                ("GOOSE_MODEL".to_string(), "stub-model".to_string()),
                (
                    "OPENAI_HOST".to_string(),
                    format!("http://127.0.0.1:{stub_port}"),
                ),
                ("OPENAI_API_KEY".to_string(), "stub-key".to_string()),
            ],
            cwd: std::env::temp_dir(),
            tasks: vec![
                "Say 'turn one done' and nothing else.".to_string(),
                "Say 'turn two done' and nothing else.".to_string(),
            ],
        };

        println!("scheduling new workflow instance {INSTANCE_ID}");
        client
            .schedule_workflow(
                diagrid_goose::WORKFLOW_NAME,
                ScheduleOptions::new()
                    .with_instance_id(INSTANCE_ID)
                    .try_with_input(input)?,
            )
            .await?;
        state.workflow_scheduled = true;
        save_state(&state);
    } else {
        println!("workflow already scheduled, polling existing instance {INSTANCE_ID}");
    }

    let metadata = client
        .wait_for_workflow_completion_with_options(
            INSTANCE_ID,
            FetchOptions::new().with_fetch_payloads(true),
            Some(Duration::from_secs(90)),
        )
        .await?;

    println!("============================================================");
    println!("workflow status: {:?}", metadata.runtime_status);
    if metadata.runtime_status == RuntimeStatus::Completed {
        if let Some(output) = &metadata.serialized_output {
            let output: diagrid_goose::GooseWorkflowOutput = serde_json::from_str(output)?;
            println!("session_id: {}", output.session_id);
            println!("turns completed: {}", output.turns.len());
            for (i, turn) in output.turns.iter().enumerate() {
                println!(
                    "  turn {}: stop_reason={} response={:?}",
                    i + 1,
                    turn.stop_reason,
                    turn.response_text
                );
            }
            println!("final_response: {:?}", output.final_response);

            let final_state = load_state();
            if final_state.run_count >= 2 && output.turns.len() == 2 {
                println!(">>> TEST PASSED: real Goose crash recovery worked after a real SIGTERM!");
            }
        }
    }
    println!("============================================================");

    worker.shutdown().await?;
    println!("worker shutdown");
    Ok(())
}
