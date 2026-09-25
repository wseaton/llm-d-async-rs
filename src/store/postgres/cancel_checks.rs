//! Coalesces workers' cancellation checks into one statement per batch: a
//! round trip per request serializes hundreds of workers on the pool.

use std::sync::Arc;
use std::time::Duration;

use deadpool_postgres::Pool;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::store::error::StoreError;
use crate::store::postgres::requests::cancelled_keys;

const MAX_BATCH: usize = 256;
const LINGER: Duration = Duration::from_millis(5);

struct Check {
    key: (String, String),
    reply: oneshot::Sender<Result<bool, Arc<StoreError>>>,
}

#[derive(Clone)]
pub(crate) struct CancelChecks {
    tx: mpsc::Sender<Check>,
}

impl CancelChecks {
    pub(crate) fn start(pool: Pool, stop: CancellationToken) -> Self {
        let (tx, rx) = mpsc::channel(MAX_BATCH * 4);
        tokio::spawn(run(pool, rx, stop));
        Self { tx }
    }

    pub(crate) async fn is_cancelled(&self, id: String, token: String) -> Result<bool, StoreError> {
        let (reply, answer) = oneshot::channel();
        self.tx
            .send(Check {
                key: (id, token),
                reply,
            })
            .await
            .map_err(|_| StoreError::Closed)?;
        answer
            .await
            .map_err(|_| StoreError::Closed)?
            .map_err(StoreError::Shared)
    }
}

async fn run(pool: Pool, mut rx: mpsc::Receiver<Check>, stop: CancellationToken) {
    loop {
        let first = tokio::select! {
            () = stop.cancelled() => return,
            first = rx.recv() => match first {
                Some(first) => first,
                None => return,
            },
        };
        let mut batch = vec![first];
        let linger = tokio::time::sleep(LINGER);
        tokio::pin!(linger);
        while batch.len() < MAX_BATCH {
            tokio::select! {
                () = &mut linger => break,
                next = rx.recv() => match next {
                    Some(check) => batch.push(check),
                    None => break,
                },
            }
        }
        let keys: Vec<(String, String)> = batch.iter().map(|c| c.key.clone()).collect();
        match cancelled_keys(&pool, &keys).await {
            Ok(cancelled) => {
                for check in batch {
                    let _ = check.reply.send(Ok(cancelled.contains(&check.key)));
                }
            }
            Err(e) => {
                let e = Arc::new(e);
                for check in batch {
                    let _ = check.reply.send(Err(Arc::clone(&e)));
                }
            }
        }
    }
}
