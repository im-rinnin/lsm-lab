use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::db::db_meta::DBMeta;
use crate::db::key::Key;
use crate::db::sstable::{SSTable, SStableWriter};
use crate::db::value::Value;

pub struct Level {
    sstables: Vec<SSTable>,
}

// pub struct LevelInfos {
//     levels: Vec<Level>,
// }

impl Level {
    pub fn get(&self, key: &Key) -> Result<Option<Value>> {
        if self.sstables.is_empty() {
            return Ok(None);
        }
        if self.last_key().unwrap().le(key) {
            return Ok(None);
        }
        // binary search sstable which key range contains key
        let position = self.sstables.partition_point(|sstable| sstable.last_key().lt(key));
        // find in sstable
        self.sstables[position].get(key)
    }
    pub fn len(&self) -> usize {
        self.sstables.len()
    }
    fn last_key(&self) -> Option<&Key> {
        self.sstables.last().map(|sstable| sstable.last_key())
    }
    fn is_empty(&self) -> bool {
        self.sstables.is_empty()
    }
    // find all sstable which key range has overlap in [start_key,end_key]
    fn key_overlap(&self, start_key: &Key, end_key: &Key) -> &[SSTable] {
        let last_key_option = self.last_key();
        if last_key_option.is_none() {
            return &[] as &[SSTable];
        }
        let last_key= last_key_option.unwrap();
        if last_key.lt(&start_key) {
            return &[] as &[SSTable];
        }
        // find first sstable which last key is greater or equal to start_key as first sstable
        let start = self.sstables.partition_point(|sstable| sstable.last_key().lt(&start_key));
        // find last sstable which last key is greater or equal to end_key as end sstable
        if last_key.le(&end_key) {
            return &self.sstables[start..];
        }
        let end = self.sstables.partition_point(|sstable| sstable.last_key().lt(&end_key));
        return &self.sstables[start..end + 2];
    }
    // all sstable size
    pub fn size(&self) -> usize {
        todo!()
    }
    // compact n-1 level sstable to this level
    pub fn compact<'a>(&mut self, input_sstable: &SSTable, stable_writer: &dyn SStableWriter) {
        // find key overlap sstable
        // build new sstable, write to stable_writer
        todo!()
    }
}

#[cfg(test)]
mod test {}
