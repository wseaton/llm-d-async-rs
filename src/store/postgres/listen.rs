//! Wakes result long-polls when any replica writes a result. A
//! notification's payload names the routes written, one per line; an empty
//! payload means any route.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_postgres::AsyncMessage;
use tokio_util::sync::CancellationToken;

use crate::store::error::StoreError;
use crate::store::postgres::RESULTS_CHANNEL;
use crate::store::postgres::connect::Database;
use crate::store::signal::ResultSignal;

const FIRST_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(5);

pub(crate) async fn run(db: Database, results: Arc<ResultSignal>, stop: CancellationToken) {
    let mut backoff = FIRST_BACKOFF;
    loop {
        match listen(&db, &results, &stop, &mut backoff).await {
            Ok(()) => return,
            Err(e) => {
                tracing::warn!(error = %e, retry_in = ?backoff, "result notifications lost; reconnecting")
            }
        }
        tokio::select! {
            () = stop.cancelled() => return,
            () = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// Listens until stopped (`Ok`) or the connection fails.
async fn listen(
    db: &Database,
    results: &ResultSignal,
    stop: &CancellationToken,
    backoff: &mut Duration,
) -> Result<(), StoreError> {
    let (client, mut connection) = db.dedicated().await?;
    let (tx, mut rx) = mpsc::unbounded_channel();
    let driver = async move {
        loop {
            match std::future::poll_fn(|cx| connection.poll_message(cx)).await {
                Some(Ok(AsyncMessage::Notification(n))) => {
                    if tx.send(n.payload().to_owned()).is_err() {
                        return Ok(());
                    }
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(StoreError::from(e)),
                None => return Err(StoreError::Closed),
            }
        }
    };
    tokio::pin!(driver);
    let statement = format!("LISTEN {RESULTS_CHANNEL}");
    tokio::select! {
        ended = &mut driver => return ended,
        listening = client.batch_execute(&statement) => listening?,
    }
    *backoff = FIRST_BACKOFF;
    // Results written while no one listened: let every long-poll look again.
    results.all();
    loop {
        tokio::select! {
            () = stop.cancelled() => return Ok(()),
            ended = &mut driver => return ended,
            Some(payload) = rx.recv() => {
                if payload.is_empty() {
                    results.all();
                } else {
                    results.written(payload.lines());
                }
            }
        }
    }
}
