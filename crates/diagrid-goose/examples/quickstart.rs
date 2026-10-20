//! Quickstart: a two-turn Goose conversation, run as a durable Dapr Workflow.
//!
//! Each turn is its own Dapr activity driving a real `goose acp` process,
//! with whatever model provider Goose is configured for (`goose configure`,
//! or `GOOSE_PROVIDER`, `GOOSE_MODEL` and the provider's API key exported in
//! the environment).
//!
//! Run it from `crates/diagrid-goose`:
//! ```bash
//! dapr run --app-id goose-quickstart --resources-path ./examples/resources \
//!     -- cargo run --example quickstart
//! ```
//! Pass your own messages, one per turn, after `--example quickstart --`.
//!
//! To see the durability: press Ctrl-C while a turn is running, then run the
//! same command again. The workflow picks up where it stopped — finished
//! turns aren't run again, and the interrupted one resumes in the same
//! Goose session.

use std::process;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dapr::workflow::{FetchOptions, RuntimeStatus, ScheduleOptions};
use diagrid_goose::{GooseWorkflowInput, GooseWorkflowOutput};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let goose = std::env::var("GOOSE_BIN").unwrap_or_else(|_| "goose".to_string());
    if process::Command::new(&goose)
        .arg("--version")
        .output()
        .is_err()
    {
        anyhow::bail!(
            "can't run `{goose}`: install the Goose CLI \
             (https://github.com/aaif-goose/goose) or set GOOSE_BIN to its path"
        );
    }

    let mut tasks: Vec<String> = std::env::args().skip(1).collect();
    if tasks.is_empty() {
        tasks = vec![
            "Pick a random fruit and reply with only its name.".to_string(),
            "In one sentence: where in the world does it grow best?".to_string(),
        ];
    }

    // Works next to `dapr run` and under `diagrid dev run` (Catalyst) alike.
    let mut client = diagrid_goose::workflow_client().await?;
    diagrid_goose::register(client.registry_mut());
    let worker = client.start_worker().await?;

    // A workflow this quickstart started but never saw finish (Ctrl-C, a
    // crash): run again, it waits on that one instead of starting over.
    let pending = std::env::temp_dir().join("diagrid-goose-quickstart.pending");
    let unfinished = match std::fs::read_to_string(&pending) {
        Ok(id)
            if client
                .fetch_workflow_metadata(&id, FetchOptions::new())
                .await
                .is_ok() =>
        {
            Some(id)
        }
        _ => None,
    };
    let instance_id = match unfinished {
        Some(id) => {
            println!("Resuming workflow {id} where it left off...");
            id
        }
        None => {
            // Goose runs unattended — tool permission requests are approved
            // automatically — so give it a directory of its own to work in.
            let cwd = std::env::temp_dir().join("diagrid-goose-quickstart");
            std::fs::create_dir_all(&cwd)?;
            let input = GooseWorkflowInput {
                program: goose,
                args: vec!["acp".to_string()],
                env: vec![],
                cwd,
                tasks,
            };
            let id = format!(
                "goose-quickstart-{}",
                SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs()
            );
            client
                .schedule_workflow(
                    diagrid_goose::WORKFLOW_NAME,
                    ScheduleOptions::new()
                        .with_instance_id(&id)
                        .try_with_input(input)?,
                )
                .await?;
            std::fs::write(&pending, &id)?;
            println!("Started workflow {id}...");
            id
        }
    };

    let metadata = client
        .wait_for_workflow_completion_with_options(
            &instance_id,
            FetchOptions::new().with_fetch_payloads(true),
            Some(Duration::from_secs(600)),
        )
        .await?;
    let _ = std::fs::remove_file(&pending);
    worker.shutdown().await?;

    if metadata.runtime_status != RuntimeStatus::Completed {
        anyhow::bail!(
            "workflow ended {:?}: {:?}",
            metadata.runtime_status,
            metadata.failure_details
        );
    }
    let input: GooseWorkflowInput =
        serde_json::from_str(metadata.serialized_input.as_deref().unwrap_or_default())?;
    let output: GooseWorkflowOutput =
        serde_json::from_str(metadata.serialized_output.as_deref().unwrap_or_default())?;
    for (i, (task, turn)) in input.tasks.iter().zip(&output.turns).enumerate() {
        println!(
            "\nTurn {}\n  you:   {task}\n  goose: {}",
            i + 1,
            turn.response_text.trim()
        );
    }
    println!(
        "\nThe conversation is saved as Goose session {0}; print it with \
         `goose session export --session-id {0}`.",
        output.session_id
    );
    Ok(())
}
