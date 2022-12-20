use std::cell::RefCell;
use std::io::Write;
use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::db::common::{KVIterItem, SortedKVIter, ValueSliceTag};
use crate::db::file_storage::{FileId, FileStorageManager};
use crate::db::key::{Key, KeySlice};
use crate::db::sstable::{SSTable, SStableIter};
use crate::db::value::{Value, ValueSlice};


// immutable
pub struct Level {
    // todo initialize to empty load by need
    // sstable_cache: SSTableMetaCache,
    sstable_file_ids: Vec<SStableFileMeta>,
    file_manager: FileStorageManager,
}

enum LevelChange {
    // add new sstable to level start from position_in_level,sstable order is same as sstable_file_metas
    ADD { level: usize, sstable_file_metas: Vec<SStableFileMeta>, position_in_level: usize },
    DELETE { level: usize, sstable_file_meta: Vec<FileId> },
}

pub struct SStableFileMeta {
    file_id: FileId,
    start_key: Key,
    end_key:Key,
}

impl Level {
    // sstables is in order
    pub fn new(sstables: Vec<FileId>, file_manager: FileStorageManager) -> Self {
        todo!()
    }
    pub fn from(sstables: Vec<SSTable>) -> Self {
        todo!()
        // Level { sstables }
    }
    pub fn get(&self, key: &Key) -> Result<Option<Value>> {
        // if self.sstables.is_empty() {
        //     return Ok(None);
        // }
        // if self.last_key().unwrap().le(key) {
        //     return Ok(None);
        // }
        // // binary search sstable which key range contains key
        // let position = self.sstables.partition_point(|sstable| sstable.last_key().lt(key));
        // // find in sstable
        // self.sstables[position].get(key)
        todo!()
    }

    pub fn pick_one_sstable_for_compact(&self) -> FileId {
        todo!()
    }

    pub fn len(&self) -> usize {
        todo!()
        // self.sstables.len()
    }
    fn last_key(&self) -> Option<&Key> {
        todo!()
        // self.sstables.last().map(|sstable| sstable.last_key())
    }
    // find all sstable which key range has overlap in [start_key,end_key]
    fn key_overlap(&self, start_key: &Key, end_key: &Key) -> &[SSTable] {
        // let last_key_option = self.last_key();
        // if last_key_option.is_none() {
        //     return &[] as &[SSTable];
        // }
        // let last_key = last_key_option.unwrap();
        // if last_key.lt(&start_key) {
        //     return &[] as &[SSTable];
        // }
        // // find first sstable which last key is greater or equal to start_key as first sstable
        // let start = self.sstables.partition_point(|sstable| sstable.last_key().lt(&start_key));
        // // find last sstable which last key is greater or equal to end_key as end sstable
        // if last_key.le(&end_key) {
        //     return &self.sstables[start..];
        // }
        // let end = self.sstables.partition_point(|sstable| sstable.last_key().lt(&end_key));
        // return &self.sstables[start..end + 1];
        todo!()
    }
    // todo compact n-1 level sstable to this level, build new sstable, return all sstable file id after compact, level is unchanged in compact
    fn compact_sstable<'a>(&'a self, mut input_sstables: Vec<&'a SSTable>) {
        //     let start_key: &Key = input_sstables.iter().map(|sstable| sstable.start_key()).min().unwrap();
        //     let end_key: &Key = input_sstables.iter().map(|sstable| sstable.last_key()).max().unwrap();
        //     // find key overlap sstable
        //     let sstable_overlap = self.key_overlap(start_key, end_key);
        //     for sstable in sstable_overlap {
        //         input_sstables.push(sstable)
        //     }
        //
        //     let mut input_sstables_iter = Vec::new();
        //     for sstable in input_sstables {
        //         let iter = sstable.iter()?;
        //         input_sstables_iter.push(iter)
        //     }
        //
        //     let mut input_sstable_iter_ref: Vec<&mut SStableIter> = input_sstables_iter.iter_mut().collect();
        //     let mut sstable_iters: Vec<&mut dyn Iterator<Item=(KeySlice, ValueSliceTag)>> = Vec::new();
        //     input_sstable_iter_ref.reverse();
        //     while !input_sstable_iter_ref.is_empty() {
        //         sstable_iters.push(input_sstable_iter_ref.pop().unwrap());
        //     }
        //
        //     // build new sstable, write to stable_writer
        //     let mut sorted_iter = SortedKVIter::new(sstable_iters);
        //     let mut res = Vec::new();
        //     loop {
        //         let (mut file, file_id) = file_manager.new_file()?;
        //         let sstable_meta = SSTable::build(&mut sorted_iter, &mut file)?;
        //         let sstable = FileBaseSSTable::new(sstable_meta, file_id, file_manager.clone());
        //         res.push(sstable);
        //         if !sorted_iter.has_next() {
        //             break;
        //         }
        //     }
        // //     todo add result sstable to this level, keep the order, remove old sstable
    }
}

impl Iterator for Level {
    type Item = (KVIterItem);

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::sync::Arc;

    use tempfile::tempdir;

    use crate::db::file_storage::FileStorageManager;
    use crate::db::key::Key;
    use crate::db::level::Level;
    use crate::db::sstable::SSTable;
    use crate::db::sstable::test::{build_sstable, build_sstable_with_special_value};
    use crate::db::value::{Value, ValueSlice};

    fn build_level() -> Level {
        let a = build_sstable(100, 200, 1);
        // println!("{:?}", a.get(&Key::new("56")).unwrap());
        let b = build_sstable(205, 300, 1);
        let c = build_sstable(305, 400, 1);

        // [100-200),[205-300),[305-400)
        Level::from(vec![a, b, c])
    }

    #[test]
    fn test_get() {
        let level = build_level();
        assert_eq!(Value::new("126"), level.get(&Key::new("126")).unwrap().unwrap());
        assert_eq!(Value::new("226"), level.get(&Key::new("226")).unwrap().unwrap());
        assert!(level.get(&Key::new("526")).unwrap().is_none());
        assert!(level.get(&Key::new("303")).unwrap().is_none());
    }

    #[test]
    fn test_key_overlap() {
        // [100-200),[205-300),[305-400)
        let level = build_level();
        let res = level.key_overlap(&Key::new("050"), &Key::new("080"));
        assert_eq!(res.len(), 1);
        assert_eq!(res.get(0).unwrap().last_key(), &Key::new("199"));

        let res = level.key_overlap(&Key::new("450"), &Key::new("480"));
        assert_eq!(res.len(), 0);

        let res = level.key_overlap(&Key::new("120"), &Key::new("280"));
        assert_eq!(res.len(), 2);
        assert_eq!(res.get(1).unwrap().last_key(), &Key::new("299"));

        let res = level.key_overlap(&Key::new("090"), &Key::new("380"));
        assert_eq!(res.len(), 3);

        let res = level.key_overlap(&Key::new("199"), &Key::new("280"));
        assert_eq!(res.len(), 2);

        let res = level.key_overlap(&Key::new("200"), &Key::new("305"));
        assert_eq!(res.len(), 2);

        let res = level.key_overlap(&Key::new("199"), &Key::new("305"));
        assert_eq!(res.len(), 3);
    }

    #[test]
    fn test_compact() {
        // // a [100,110) delete 109 set 105 to X b[108,115] set 113 to Z set 109 to Z
        // // c [105,108) ,d [110,115) set 112 to Y, e[122,124)
        // // sstable create order a>b>c....>e, so priority order is a>b>...e
        //
        // let mut special_value_map = HashMap::new();
        // special_value_map.insert(109, None);
        // special_value_map.insert(105, Some(Value::new("X")));
        //
        // let a = build_sstable_with_special_value(100, 110, 1, special_value_map);
        //
        // let mut special_value_map = HashMap::new();
        // special_value_map.insert(109, Some(Value::new("Z")));
        // special_value_map.insert(113, Some(Value::new("Z")));
        // let b = build_sstable_with_special_value(108, 115, 1, special_value_map);
        //
        // let c = build_sstable(105, 108, 1);
        //
        // let mut special_value_map = HashMap::new();
        // special_value_map.insert(112, Some(Value::new("Y")));
        // let d = build_sstable_with_special_value(110, 115, 1, special_value_map);
        //
        // let e = build_sstable(122, 124, 1);
        //
        // let mut level = Level::from(vec![c, d, e]);
        // let mut file = tempfile::tempfile().unwrap();
        //
        // let dir = tempdir().unwrap();
        // let mut file_manager = FileStorageManager::new(dir.path());
        //
        // let mut file_sstable = level.compact_sstable(vec![&a, &b], &mut file_manager).unwrap();
        // assert_eq!(file_sstable.len(), 1);
        // let sstable = file_sstable.pop().unwrap().new_sstable().unwrap();
        // let expect = "(key: 100,value: 100)(key: 101,value: 101)(key: 102,value: 102)(key: 103,value: 103)(key: 104,value: 104)(key: 105,value: X)(key: 106,value: 106)(key: 107,value: 107)(key: 108,value: 108)(key: 109,value: None)(key: 110,value: 110)(key: 111,value: 111)(key: 112,value: 112)(key: 113,value: Z)(key: 114,value: 114)";
        // assert_eq!(sstable.to_string(), expect);
    }

    #[test]
    fn test_add_sstable_to_empty_level() {}

    #[test]
    fn test_add_sstable_to_level_and_compact() {}
}
