type Release = Box<dyn FnOnce() + Send>;

/// Reservations taken by gates for one request, given back in reverse order
/// when dropped. Holding a `Releases` is holding the capacity.
#[derive(Default)]
pub struct Releases(Vec<Release>);

impl Releases {
    pub fn push(&mut self, release: impl FnOnce() + Send + 'static) {
        self.0.push(Box::new(release));
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Gives back every reservation taken after the first `keep`.
    pub fn release_from(&mut self, keep: usize) {
        while self.0.len() > keep {
            if let Some(release) = self.0.pop() {
                release();
            }
        }
    }
}

impl Drop for Releases {
    fn drop(&mut self) {
        self.release_from(0);
    }
}

impl std::fmt::Debug for Releases {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Releases({})", self.0.len())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::gate::release::Releases;

    #[test]
    fn releases_run_in_reverse_order_once() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut r = Releases::default();
        for i in 0..3 {
            let log = Arc::clone(&log);
            r.push(move || log.lock().unwrap().push(i));
        }
        r.release_from(1);
        assert_eq!(*log.lock().unwrap(), [2, 1]);
        assert_eq!(r.len(), 1);
        drop(r);
        assert_eq!(*log.lock().unwrap(), [2, 1, 0]);
    }
}
