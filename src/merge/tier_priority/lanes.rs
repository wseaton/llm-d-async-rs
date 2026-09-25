use std::collections::{HashMap, VecDeque};

pub const LANES: usize = 6;

/// One priority lane: FIFO per source, round-robin across sources.
struct Lane<T> {
    order: Vec<u64>,
    queues: HashMap<u64, VecDeque<T>>,
    next: usize,
    size: usize,
}

impl<T> Default for Lane<T> {
    fn default() -> Self {
        Self {
            order: Vec::new(),
            queues: HashMap::new(),
            next: 0,
            size: 0,
        }
    }
}

impl<T> Lane<T> {
    fn push(&mut self, source: u64, item: T) {
        let queue = self.queues.entry(source).or_insert_with(|| {
            self.order.push(source);
            VecDeque::new()
        });
        queue.push_back(item);
        self.size += 1;
    }

    fn pop(&mut self) -> Option<T> {
        let source = *self.order.get(self.next)?;
        let queue = self.queues.get_mut(&source)?;
        let item = queue.pop_front()?;
        self.size -= 1;
        if queue.is_empty() {
            self.queues.remove(&source);
            self.order.remove(self.next);
            if self.next >= self.order.len() {
                self.next = 0;
            }
        } else {
            self.next = (self.next + 1) % self.order.len();
        }
        Some(item)
    }
}

/// Six strict-priority lanes, each bounded to `capacity` items.
pub struct Lanes<T> {
    lanes: [Lane<T>; LANES],
    capacity: usize,
    total: usize,
}

impl<T> Lanes<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            lanes: Default::default(),
            capacity,
            total: 0,
        }
    }

    /// Queues `item`, or hands it back when the lane is full.
    pub fn try_push(&mut self, lane: usize, source: u64, item: T) -> Result<(), T> {
        let Some(l) = self.lanes.get_mut(lane) else {
            return Err(item);
        };
        if l.size >= self.capacity {
            return Err(item);
        }
        l.push(source, item);
        self.total += 1;
        Ok(())
    }

    /// The next item of the highest non-empty lane.
    pub fn pop(&mut self) -> Option<T> {
        let item = self.lanes.iter_mut().find_map(Lane::pop)?;
        self.total -= 1;
        Some(item)
    }

    pub fn is_empty(&self) -> bool {
        self.total == 0
    }
}

#[cfg(test)]
mod tests {
    use crate::merge::tier_priority::lanes::Lanes;

    #[test]
    fn strict_priority_then_round_robin() {
        let mut l = Lanes::new(10);
        for (lane, source, item) in [
            (5, 1, "b1"),
            (2, 1, "a1"),
            (2, 1, "a2"),
            (2, 2, "c1"),
            (0, 3, "top"),
        ] {
            l.try_push(lane, source, item).unwrap();
        }
        let order: Vec<_> = std::iter::from_fn(|| l.pop()).collect();
        assert_eq!(order, ["top", "a1", "c1", "a2", "b1"]);
        assert!(l.is_empty());
    }

    #[test]
    fn full_lane_hands_the_item_back() {
        let mut l = Lanes::new(1);
        l.try_push(3, 1, 1).unwrap();
        assert_eq!(l.try_push(3, 2, 2), Err(2));
        assert_eq!(l.try_push(4, 2, 3), Ok(()));
        assert_eq!(l.try_push(6, 2, 4), Err(4));
    }

    #[test]
    fn round_robin_survives_sources_draining() {
        let mut l = Lanes::new(10);
        for (s, i) in [(1, 1), (2, 2), (3, 3), (1, 4), (3, 5)] {
            l.try_push(0, s, i).unwrap();
        }
        let order: Vec<_> = std::iter::from_fn(|| l.pop()).collect();
        assert_eq!(order, [1, 2, 3, 4, 5]);
    }
}
