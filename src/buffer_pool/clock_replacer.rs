use crate::buffer_pool::{FrameId, Replacer};
use serde::{Deserialize, Serialize};

pub struct ClockReplacer {
    list: Vec<(FrameId, bool)>,
    current: usize,
}

impl ClockReplacer {
    pub fn new() -> ClockReplacer {
        ClockReplacer {
            list: Vec::new(),
            current: 0,
        }
    }

    fn remove(&mut self, index: usize) {
        self.list.remove(index);
        if self.current >= self.list.len() {
            self.current = 0;
        }
    }
}

impl Replacer for ClockReplacer {
    fn victim(&mut self) -> Option<FrameId> {
        if self.list.is_empty() {
            return None;
        }

        loop {
            if self.list[self.current].1 {
                self.list[self.current].1 = false;
                self.current = (self.current + 1) % self.list.len();
            } else {
                let frame_id = self.list[self.current].0;
                self.remove(self.current);
                return Some(frame_id);
            }
        }
    }

    fn unpin(&mut self, id: FrameId) {
        let has = self.list.iter().any(|(i, _)| *i == id);
        if !has {
            self.list.push((id, true));
        }
    }

    fn pin(&mut self, id: FrameId) {
        if let Some(index) = self.list.iter().position(|&e| e.0 == id) {
            self.remove(index);
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct ClockReplacerRep {
    #[serde(rename = "ClockHand")]
    pub clock_hand: i32,
    #[serde(rename = "Clock")]
    pub clock: Vec<ClockValue>,
}

#[derive(Deserialize, Serialize)]
pub struct ClockValue {
    #[serde(rename = "ClockFrame")]
    clock_frame: i32,
    #[serde(rename = "ReferenceValue")]
    reference_value: bool,
}

impl ClockReplacer {
    pub fn response(&self) -> ClockReplacerRep {
        let mut clock: Vec<ClockValue> = Vec::new();
        for (id, value) in self.list.iter() {
            clock.push(ClockValue {
                clock_frame: *id,
                reference_value: *value,
            });
        }
        return ClockReplacerRep {
            clock_hand: self.current as i32,
            clock,
        };
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer_pool::{ClockReplacer, Replacer};

    #[test]
    fn clock_replacer() {
        let mut r = ClockReplacer::new();
        r.unpin(1);
        r.unpin(2);
        r.unpin(3);
        r.unpin(4);
        r.unpin(5);
        r.unpin(6);
        r.unpin(1);

        assert_eq!(6, r.list.len());
        assert_eq!(Some(1), r.victim());
        assert_eq!(Some(2), r.victim());
        assert_eq!(Some(3), r.victim());

        r.pin(3);
        r.pin(4);
        assert_eq!(2, r.list.len());

        r.unpin(4);
        assert_eq!(Some(5), r.victim());
        assert_eq!(Some(6), r.victim());
        assert_eq!(Some(4), r.victim());
    }

    #[test]
    fn buffer_pool_manager() {
    }
}
