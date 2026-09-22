//! Crash recovery test for the Rust Dapr Workflow SDK.
//!
//! Three activities run in sequence: `node_one` -> `node_two` -> `node_three`.
//! On the first run, `node_two` blocks for ~20s (printing its pid) instead of
//! completing immediately, giving an external `kill -TERM <pid>` a real
//! window to land while the activity is genuinely in flight — this is not
//! a self-inflicted `os::process::exit`, it's an actual signal from outside
//! the process, matching what a Kubernetes pod kill sends.
//!
//! Usage:
//! ```bash
//! # Clean up any previous run's state first:
//! rm -f /tmp/diagrid_goose_crash_test_state.json
//!
//! # Run 1 (will block in node_two — SIGTERM the printed pid from another shell):
//! dapr run --app-id diagrid-goose-crash-test \
//!     --resources-path ./examples/resources \
//!     -- cargo run --example crash_recovery
//!
//! # Run 2 (Dapr resumes the interrupted activity and completes the workflow):
//! dapr run --app-id diagrid-goose-crash-test \
//!     --resources-path ./examples/resources \
//!     -- cargo run --example crash_recovery
//! ```

use std::fs;
use std::path::Path;
use std::process;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use dapr::workflow::{
    ActivityContext, FetchOptions, RegistryExt, RuntimeStatus, ScheduleOptions, WorkflowClient,
    WorkflowContext, WorkflowContextExt,
};

const STATE_FILE: &str = "/tmp/diagrid_goose_crash_test_state.json";
const INSTANCE_ID: &str = "goose-crash-recovery-test";
const TEST_WORKFLOW: &str = "CrashTestWorkflow";

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
struct TestState {
    run_count: i32,
    node1_executed: bool,
    node2_executed: bool,
    node3_executed: bool,
    workflow_scheduled: bool,
}

fn load_state() -> TestState {
    if Path::new(STATE_FILE).exists() {
        let data = fs::read_to_string(STATE_FILE).unwrap_or_default();
        serde_json::from_str(&data).unwrap_or_default()
    } else {
        TestState::default()
    }
}

fn save_state(state: &TestState) {
    fs::write(STATE_FILE, serde_json::to_string_pretty(state).unwrap()).unwrap();
}

async fn crash_test_workflow(ctx: WorkflowContext) -> dapr::workflow::Result<Option<String>> {
    let _input: i32 = ctx.get_input_typed()?;
    let r1: String = ctx.call_activity_typed("node_one", 0).await?;
    let r2: String = ctx.call_activity_typed("node_two", 0).await?;
    let r3: String = ctx.call_activity_typed("node_three", 0).await?;
    Ok(Some(serde_json::to_string(&format!("{r1} | {r2} | {r3}"))?))
}

async fn node_one(
    _ctx: ActivityContext,
    _input: Option<String>,
) -> dapr::workflow::Result<Option<String>> {
    println!(">>> NODE 1 EXECUTING");
    let mut state = load_state();
    state.node1_executed = true;
    save_state(&state);
    println!(">>> NODE 1 COMPLETED");
    Ok(Some(serde_json::to_string(&"node1_done".to_string())?))
}

async fn node_two(
    _ctx: ActivityContext,
    _input: Option<String>,
) -> dapr::workflow::Result<Option<String>> {
    println!(">>> NODE 2 EXECUTING");
    let state = load_state();

    if state.run_count == 1 {
        println!(
            ">>> NODE 2: SLEEPING FOR SIGTERM TEST (pid={})",
            process::id()
        );
        println!(">>> Send: kill -TERM {}", process::id());
        for i in 0..20 {
            println!(">>> NODE 2 still sleeping ({}/20s)", i + 1);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        println!(">>> NODE 2: woke up without being killed!");
    }

    let mut state = load_state();
    state.node2_executed = true;
    save_state(&state);
    println!(">>> NODE 2 COMPLETED");
    Ok(Some(serde_json::to_string(&"node2_done".to_string())?))
}

async fn node_three(
    _ctx: ActivityContext,
    _input: Option<String>,
) -> dapr::workflow::Result<Option<String>> {
    println!(">>> NODE 3 EXECUTING");
    let mut state = load_state();
    state.node3_executed = true;
    save_state(&state);
    println!(">>> NODE 3 COMPLETED");
    Ok(Some(serde_json::to_string(&"node3_done".to_string())?))
}

#[tokio::main]
async fn main() -> dapr::workflow::Result<()> {
    env_logger::init();

    let mut state = load_state();
    state.run_count += 1;
    save_state(&state);

    println!("============================================================");
    println!("RUN #{}  (pid={})", state.run_count, process::id());
    println!("============================================================");
    println!(
        "Previous state: node1={} node2={} node3={}",
        state.node1_executed, state.node2_executed, state.node3_executed
    );
    println!("workflow_scheduled={}", state.workflow_scheduled);
    println!("============================================================");

    let mut client = WorkflowClient::new().await?;
    client
        .registry_mut()
        .add_workflow(TEST_WORKFLOW, crash_test_workflow);
    client.registry_mut().add_activity("node_one", node_one);
    client.registry_mut().add_activity("node_two", node_two);
    client.registry_mut().add_activity("node_three", node_three);

    let worker = client.start_worker().await?;
    println!("worker started");

    if !state.workflow_scheduled {
        println!("scheduling new workflow instance {INSTANCE_ID}");
        client
            .schedule_workflow(
                TEST_WORKFLOW,
                ScheduleOptions::new()
                    .with_instance_id(INSTANCE_ID)
                    .with_input(0),
            )
            .await?;
        let mut state = load_state();
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

    println!("workflow status: {:?}", metadata.runtime_status);
    if metadata.runtime_status == RuntimeStatus::Completed {
        println!("output: {:?}", metadata.serialized_output);
    }

    let final_state = load_state();
    println!("============================================================");
    println!("VERIFICATION:");
    println!("node1_executed={}", final_state.node1_executed);
    println!("node2_executed={}", final_state.node2_executed);
    println!("node3_executed={}", final_state.node3_executed);
    println!("total_runs={}", final_state.run_count);
    if final_state.run_count >= 2
        && final_state.node1_executed
        && final_state.node2_executed
        && final_state.node3_executed
        && metadata.runtime_status == RuntimeStatus::Completed
    {
        println!(">>> TEST PASSED: crash recovery worked after a real SIGTERM!");
    }
    println!("============================================================");

    worker.shutdown().await?;
    println!("worker shutdown");

    Ok(())
}
