use std::fs::File;
use std::path::{Path, PathBuf};

use crate::db::file_storage::{FileId, FileStorageManager};

pub struct MetaLog {
    path: PathBuf,
    file: File,
}

struct Entry {
    size: usize,
    data: Vec<u8>,
    timestamp: u64,
}

pub struct LogStoreIterator {
    file: File,
    timestamp: u64,
}

impl Iterator for LogStoreIterator {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}


impl MetaLog {
    pub fn new(file: File) -> Self {
        todo!()
    }

    pub fn add_data(&mut self, data: &Vec<u8>) {
        todo!()
    }

    pub fn iter(&self, timestamp: u64) -> LogStoreIterator {
        todo!()
    }
    // for db start
    pub fn iter_all(&self) -> LogStoreIterator {
        todo!()
    }
}