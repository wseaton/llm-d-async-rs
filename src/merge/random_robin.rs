use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt, StreamMap};
use tokio_util::sync::CancellationToken;

use crate::config::merge_policy::Fairness;
use crate::dispatch::message::{Claimed, Dispatch, SourceMeta};
use crate::merge::Source;
use crate::merge::stamp::base_headers;
use crate::telemetry::metrics::Metrics;

type Tagged = Pin<Box<dyn Stream<Item = (Arc<SourceMeta>, Claimed)> + Send>>;

/// Takes the next request from a randomly chosen ready queue. `StreamMap`
/// starts every poll at a random stream, and drops a stream once its queue's
/// sender is gone.
pub async fn run(
    mut control: mpsc::UnboundedReceiver<Source>,
    merged: mpsc::Sender<Dispatch>,
    fairness: Fairness,
    metrics: Arc<Metrics>,
    cancel: CancellationToken,
) {
    let mut sources: StreamMap<u64, Tagged> = StreamMap::new();
    let mut next_id = 0u64;
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            Some(source) = control.recv() => {
                let meta = source.meta;
                let stream = ReceiverStream::new(source.rx).map(move |c| (Arc::clone(&meta), c));
                sources.insert(next_id, Box::pin(stream));
                next_id += 1;
            }
            Some((_, (meta, claimed))) = sources.next(), if !sources.is_empty() => {
                let dispatch = Dispatch {
                    url: meta.url_for(&claimed.envelope),
                    headers: base_headers(&meta, &claimed.envelope, &fairness),
                    source: meta,
                    claimed,
                };
                let labels = dispatch.source.labels.clone();
                metrics.queue_depth_inc(&labels);
                let sent = tokio::select! {
                    biased;
                    () = cancel.cancelled() => false,
                    sent = merged.send(dispatch) => sent.is_ok(),
                };
                if !sent {
                    metrics.queue_depth_dec(&labels);
                    return;
                }
            }
        }
    }
}
