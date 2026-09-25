use std::process::ExitCode;

use clap::Parser;
use llm_d_async::config::cli::Cli;
use llm_d_async::runner;
use llm_d_async::telemetry::tracing as telemetry;

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::error!(error = %e, "failed to listen for SIGINT");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sigterm) => {
                sigterm.recv().await;
            }
            Err(e) => {
                tracing::error!(error = %e, "failed to listen for SIGTERM");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let guard = match telemetry::init(cli.verbosity) {
        Ok(guard) => guard,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::FAILURE;
        }
    };
    let result = runner::run(cli, shutdown_signal()).await;
    let code = match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!(error = %e, "llm-d-async failed");
            ExitCode::FAILURE
        }
    };
    guard.shutdown();
    code
}
