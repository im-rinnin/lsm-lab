use std::fs::File;

use anyhow::Result;

use crate::db::key::Key;
use crate::db::value::Value;

pub struct MemtableLog {
    file: File,
}

impl MemtableLog {
    pub fn new(file: File) -> Self {
        todo!()
    }
    pub fn add(&mut self, key: &Key, value: &Value) -> Result<()> {
        todo!()
    }
}

pub struct MemtableLogReader {
    file: File,
}

impl MemtableLogReader {
    pub fn new(file: File) -> Self {
        todo!()
    }
}

impl Iterator for MemtableLogReader {
    type Item = (Key, Value);

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
