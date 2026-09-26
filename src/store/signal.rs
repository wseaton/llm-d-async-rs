//! Wakes result long-polls by route, so a write wakes only the polls that
//! can find it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use tokio::sync::Notify;

#[derive(Default)]
pub struct ResultSignal {
    routes: Mutex<HashMap<String, Arc<Notify>>>,
}

/// A long-poll's registration for writes to its route. Dropping the last
/// one for a route forgets the route.
pub struct RouteWatch {
    signal: Arc<ResultSignal>,
    route: String,
    notify: Arc<Notify>,
}

impl RouteWatch {
    pub fn notify(&self) -> &Notify {
        &self.notify
    }
}

impl Drop for RouteWatch {
    fn drop(&mut self) {
        let mut routes = self
            .signal
            .routes
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if Arc::strong_count(&self.notify) == 2 {
            routes.remove(&self.route);
        }
    }
}

impl ResultSignal {
    pub fn watch(self: &Arc<Self>, route: &str) -> RouteWatch {
        let notify = Arc::clone(
            self.routes
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entry(route.to_owned())
                .or_default(),
        );
        RouteWatch {
            signal: Arc::clone(self),
            route: route.to_owned(),
            notify,
        }
    }

    /// Results were written to `routes`.
    pub fn written<'a>(&self, routes: impl IntoIterator<Item = &'a str>) {
        let watched = self.routes.lock().unwrap_or_else(PoisonError::into_inner);
        for route in routes {
            if let Some(notify) = watched.get(route) {
                notify.notify_waiters();
            }
        }
    }

    /// Results may have been written anywhere.
    pub fn all(&self) {
        for notify in self
            .routes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
        {
            notify.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::store::signal::ResultSignal;

    async fn woken(watch: &crate::store::signal::RouteWatch, by: impl FnOnce()) -> bool {
        let notified = watch.notify().notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        by();
        tokio::time::timeout(Duration::from_millis(50), notified)
            .await
            .is_ok()
    }

    #[tokio::test]
    async fn a_write_wakes_its_route_only() {
        let signal = Arc::new(ResultSignal::default());
        let a = signal.watch("a");
        let b = signal.watch("b");
        assert!(woken(&a, || signal.written(["a"])).await);
        assert!(!woken(&b, || signal.written(["a"])).await);
        assert!(woken(&b, || signal.written(["c", "b"])).await);
        assert!(woken(&a, || signal.all()).await);
        assert!(woken(&b, || signal.all()).await);
    }

    #[tokio::test]
    async fn routes_are_forgotten_with_their_last_watch() {
        let signal = Arc::new(ResultSignal::default());
        let first = signal.watch("a");
        let second = signal.watch("a");
        drop(first);
        assert!(woken(&second, || signal.written(["a"])).await);
        drop(second);
        assert!(signal.routes.lock().unwrap().is_empty());
    }
}
