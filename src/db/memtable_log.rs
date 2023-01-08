use anyhow::Result;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use rmp_serde::{Deserializer, Serializer};
use serde::Serialize;
use std::cmp::max;
use std::fs::File;
use std::io::{Read, Seek, Write};

use crate::db::key::Key;
use crate::db::value::Value;

use super::db_metrics::TimeRecorder;
use super::key::KEY_SIZE_LIMIT;
use super::value::VALUE_SIZE_LIMIT;

pub struct MemtableLog {
    file: File,
    buffer: Vec<u8>,
}

struct KVEntry {
    size: usize,
    key: Key,
    value: Value,
}

impl MemtableLog {
    pub fn new(file: File) -> Self {
        MemtableLog {
            file,
            buffer: Vec::with_capacity(KEY_SIZE_LIMIT + VALUE_SIZE_LIMIT),
        }
    }
    pub fn add(&mut self, key: &Key, value: &Value) -> Result<()> {
        key.serialize(&mut Serializer::new(&mut self.file))?;
        value.serialize(&mut Serializer::new(&mut self.file))?;
        Ok(())
    }
    pub fn sync_all(&mut self) -> Result<()> {
        let time = TimeRecorder::new("memtable_log.flush_time");
        self.file.sync_data()?;
        Ok(())
    }
}

pub struct MemtableLogReader {
    file: File,
    file_size: u64,
}

impl MemtableLogReader {
    pub fn new(file: File) -> Result<Self> {
        let meta = file.metadata()?;
        let file_size = meta.len();

        Ok(MemtableLogReader { file, file_size })
    }
}

impl Iterator for MemtableLogReader {
    type Item = (Key, Value);

    fn next(&mut self) -> Option<Self::Item> {
        let position = self.file.stream_position().unwrap();

        if position == self.file_size {
            return None;
        }

        let key: Key = rmp_serde::decode::from_read(&mut self.file).unwrap();
        let value: Value = rmp_serde::decode::from_read(&mut self.file).unwrap();

        Some((key, value))
    }
}

#[cfg(test)]
mod test {
    use std::fs::File;

    use tempfile::{tempdir, tempfile};

    use crate::db::{
        common::init_test_log_as_debug,
        db_metrics::{init_metric, TimeRecorder},
        key::Key,
        memtable::MemtableIter,
        value::Value,
    };

    use super::{MemtableLog, MemtableLogReader};

    #[test]
    fn simple_test() {
        init_test_log_as_debug();
        let r = init_metric();
        let dir = tempdir().unwrap();
        let path = dir.into_path().join("test");
        let file = File::create(&path).unwrap();

        let mut log = MemtableLog::new(file);
        let key_1 = Key::new("1");
        let value_1 = Value::new("1");

        let key_2 = Key::new("2");
        let value_2 = Value::new("2");

        log.add(&key_1, &value_1).unwrap();
        log.add(&key_2, &value_2).unwrap();

        log.sync_all().unwrap();

        let iter = MemtableLogReader::new(File::open(&path).unwrap()).unwrap();

        for (k, v) in iter {
            assert_eq!(k.data(), v.data())
        }
        for i in 0..100 {
            log.add(&key_1, &value_1).unwrap();
            log.sync_all();
        }
        r.log_current_metric();
    }
}
