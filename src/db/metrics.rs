use std::sync::atomic::{self, AtomicU64, Ordering};

use log::debug;

#[derive(Debug)]
pub struct DBMetric {
    level_0_file_number: Vec<AtomicU64>,
}

impl DBMetric {
    pub fn new() -> Self {
        DBMetric {
            level_0_file_number: vec![
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
            ],
        }
    }

    pub fn set_level_n_file_number(&self, size: u64, level: usize) {
        debug!("set_level {:} file_number to {}", level, size);
        self.level_0_file_number
            .get(level)
            .unwrap()
            .store(size, Ordering::SeqCst);
    }

    pub fn get_level_n_file_number(&self, level: usize) -> u64 {
        let res = self
            .level_0_file_number
            .get(level)
            .unwrap()
            .load(Ordering::SeqCst);
        res
    }
}
