# Durable Workflows for AI agents (Rust)

**Make your AI agents resilient to failure and outages**

This is the Rust counterpart to [`python-ai`](https://github.com/diagridio/python-ai), [`dotnet-ai`](https://github.com/diagridio/dotnet-ai), and [`typescript-ai`](https://github.com/diagridio/typescript-ai): an extension SDK for the open-source [Dapr](https://github.com/dapr/dapr) project that wraps AI agent frameworks in Dapr Workflows so agents can recover from failures, persist state across restarts, and scale effectively — for agent frameworks whose runtime lives natively in Rust rather than being callable from Python/.NET/TypeScript.

## Status

No crates published yet. First integration, `diagrid-goose`, is implemented and validated locally against Dapr and a real `goose` binary — see [`crates/diagrid-goose`](crates/diagrid-goose).

Built on [`dapr/rust-sdk`](https://github.com/dapr/rust-sdk)'s Workflow module (Alpha), pinned to the exact published `dapr = "0.19.0"` release on crates.io — validated locally for activity execution, retry policies, and crash recovery (process killed mid-activity resumes correctly on restart).

### `diagrid-goose`

[Goose](https://github.com/aaif-goose/goose)'s agent loop (tool calling, extensions/MCP, session state machine) lives entirely in Rust, but not as an importable library — `goose-agent` and friends are internal crates of the `aaif-goose/goose` monorepo, not published to crates.io. `diagrid-goose` depends on none of them and requires no change to Goose itself: it drives an unmodified `goose acp` process purely over Goose's stable, already-published [Agent Client Protocol](https://agentclientprotocol.com/) interface.

One Dapr activity = one Goose turn (one `session/prompt` round trip against a freshly spawned process). Session continuity across turns rides Goose's own `session/load` support, so each turn is an independent, freshly spawned activity with no shared process or connection state that a crash could strand. A retried turn resumes from what Goose's session already holds: Goose saves a turn's prompt before calling the model, so the retry never sends that prompt to the model a second time, and an attempt that finished before Dapr recorded it isn't run again. A turn that fails outright (Goose exits, its connection breaks, or it returns an error) gets up to 3 attempts in all, 5s and then 10s apart; a model provider that needs authentication fails the workflow straight away instead.

Verified end to end against local Dapr with [Goose v1.54.0](https://github.com/aaif-goose/goose/releases/tag/v1.54.0): `kill -TERM` sent to the worker while turn 2's model call is in flight, a fresh process resumes the same Goose session and completes the turn without re-running turn 1 or repeating turn 2's prompt — see `crates/diagrid-goose/examples/goose_crash_recovery.rs`. `crates/diagrid-goose/tests/goose_resume.rs` pins down the Goose behavior this relies on; re-run it on every Goose upgrade (`GOOSE_BIN=/path/to/goose cargo test -p diagrid-goose -- --ignored`).

## Getting started

### Prerequisites

- **Rust 1.88 or newer**, e.g. via [rustup](https://rustup.rs) — check with `rustc --version`.
- **Dapr**, set up for local development: [install the Dapr CLI](https://docs.dapr.io/getting-started/install-dapr-cli/), then run `dapr init`, which starts the Redis, placement and scheduler containers (Docker required) that Dapr Workflows run on. Validated with Dapr 1.18.
- **Goose**, with a model provider configured. The installer walks you through `goose configure` (choose a provider and model, enter its API key), and you can re-run `goose configure` any time:

  ```bash
  curl -fsSL https://github.com/aaif-goose/goose/releases/download/stable/download_cli.sh | bash
  ```

  `diagrid-goose` is validated against Goose v1.54.0; to install exactly that version, change the end of the command to `| GOOSE_VERSION=v1.54.0 bash`. Rather than running `goose configure`, you can also configure Goose through the environment, e.g. for OpenAI:

  ```bash
  export GOOSE_PROVIDER=openai GOOSE_MODEL=gpt-4.1-mini OPENAI_API_KEY=sk-...
  ```

  On servers and in containers, use the environment and also set `GOOSE_DISABLE_KEYRING=1`, so Goose never reaches for a system keyring that isn't there — Goose sets the same flag for the unattended runs its own scheduler starts.

### Run the quickstart

```bash
git clone https://github.com/diagridio/rust-ai
cd rust-ai/crates/diagrid-goose
dapr run --app-id goose-quickstart --resources-path ./examples/resources \
    -- cargo run --example quickstart
```

[`examples/quickstart.rs`](crates/diagrid-goose/examples/quickstart.rs) runs a two-turn conversation with Goose as a Dapr Workflow, one activity per turn, and prints something like:

```text
Turn 1
  you:   Pick a random fruit and reply with only its name.
  goose: Mango

Turn 2
  you:   In one sentence: where in the world does it grow best?
  goose: Mangoes grow best in the tropics, especially India.

The conversation is saved as Goose session 20261013_1; print it with `goose session export --session-id 20261013_1`.
```

To send your own messages, pass one per turn:

```bash
dapr run --app-id goose-quickstart --resources-path ./examples/resources \
    -- cargo run --example quickstart -- "Plan a weekend in Lisbon." "Make it cheaper."
```

### See it survive a crash

Run the quickstart again, press Ctrl-C while Goose is still answering, then run the exact same command once more. The workflow picks up where it stopped: turns that already finished aren't run again, and the one you interrupted is completed.

### Use it in your own app

`diagrid-goose` isn't on crates.io yet, so depend on it from git:

```toml
[dependencies]
diagrid-goose = { git = "https://github.com/diagridio/rust-ai" }
dapr = { version = "0.19.0", features = ["workflow"] }
tokio = { version = "1", features = ["full"] }
serde_json = "1"
anyhow = "1"
```

```rust
use std::time::Duration;

use dapr::workflow::{FetchOptions, RuntimeStatus, ScheduleOptions, WorkflowClient};
use diagrid_goose::{GooseWorkflowInput, GooseWorkflowOutput, WORKFLOW_NAME};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Register the Goose workflow and its per-turn activity, and start serving them.
    let mut client = WorkflowClient::new().await?;
    diagrid_goose::register(client.registry_mut());
    let worker = client.start_worker().await?;

    let input = GooseWorkflowInput {
        program: "goose".to_string(),
        args: vec!["acp".to_string()],
        // Extra environment for Goose, e.g. provider settings.
        env: vec![],
        // Where Goose works. Its tool calls are approved automatically, so it
        // may change files here.
        cwd: std::env::temp_dir(),
        // One message per turn, all in the same Goose session.
        tasks: vec![
            "Write a haiku about durable execution.".to_string(),
            "Now translate it into French.".to_string(),
        ],
    };
    let instance_id = client
        .schedule_workflow(WORKFLOW_NAME, ScheduleOptions::new().try_with_input(input)?)
        .await?;

    let done = client
        .wait_for_workflow_completion_with_options(
            &instance_id,
            FetchOptions::new().with_fetch_payloads(true),
            Some(Duration::from_secs(600)),
        )
        .await?;
    anyhow::ensure!(
        done.runtime_status == RuntimeStatus::Completed,
        "workflow ended {:?}: {:?}",
        done.runtime_status,
        done.failure_details
    );
    let output: GooseWorkflowOutput =
        serde_json::from_str(done.serialized_output.as_deref().unwrap_or_default())?;
    println!("{}", output.final_response);

    worker.shutdown().await?;
    Ok(())
}
```

Run it next to a Dapr sidecar: `dapr run --app-id my-agent -- cargo run`. The state store `dapr init` sets up already works for Dapr Workflows; pass `--resources-path` to use your own components instead.

### Good to know

- **Goose runs unattended.** Tool permission requests are approved automatically, so point `cwd` at a directory you're comfortable with Goose changing.
- **Provider settings can be per workflow.** `env` is added on top of the worker's own environment for every Goose process the workflow starts, so it can carry `GOOSE_PROVIDER`, `GOOSE_MODEL` or an API key.
- **Goose's session store has to outlive the worker.** Goose keeps sessions in its own SQLite store (`GOOSE_PATH_ROOT`, by default under `~/.local/share/goose`), and a turn can only resume against the store its session started in — in a container, put it on a persistent volume.

### Examples

| Example | What it shows |
| --- | --- |
| [`quickstart`](crates/diagrid-goose/examples/quickstart.rs) | A two-turn Goose conversation as a durable workflow, using your configured model provider. |
| [`goose_crash_recovery`](crates/diagrid-goose/examples/goose_crash_recovery.rs) | Self-checking crash test: the worker is killed in the middle of a turn, and a restart resumes it. Uses a built-in stand-in for the model, so no API key needed; takes two runs (steps in the file). |
| [`crash_recovery`](crates/diagrid-goose/examples/crash_recovery.rs) | The same kind of crash test for a plain Dapr workflow, without Goose. |

## Community

Have questions, hit a bug, or want to share what you're building? Join the [Diagrid Community Discord](https://diagrid.ws/diagrid-community) to connect with the team and other users.
