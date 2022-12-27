use std::cell::RefCell;
use std::fs::File;
use std::io::Write;
use std::num::NonZeroUsize;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use lru::LruCache;
use serde::{Deserialize, Serialize};

use crate::db::common::{KVIterItem, SortedKVIter, ValueSliceTag};
use crate::db::file_storage::{FileId, FileStorageManager, ThreadSafeFileManager};
use crate::db::key::{Key, KeySlice};
use crate::db::memtable::Memtable;
use crate::db::sstable::{SSTable, SStableBlockMeta, SStableIter};
use crate::db::value::{Value, ValueSlice};

pub type SSTableBlockMetaCache = LruCache<FileId, Arc<SStableBlockMeta>>;
pub type ThreadSafeSSTableMetaCache = Arc<Mutex<SSTableBlockMetaCache>>;

// immutable, own by version
pub struct Level {
    sstable_cache: ThreadSafeSSTableMetaCache,
    sstable_file_metas: Vec<SStableFileMeta>,
    file_manager: ThreadSafeFileManager,
    home_path: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum LevelChange {
    // add new sstable to level start from position_in_level,sstable order is same as sstable_file_metas
    MEMTABLE_COMPACT { sstable_file_metas: Vec<SStableFileMeta> },
    LEVEL_COMPACT {
        // compact 1 to 2, compact_from_leve is 1
        compact_from_level: usize,
        remove_sstable_file_ids: Vec<FileId>,
        add_sstable_file_metas: Vec<SStableFileMeta>,
    },
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct SStableFileMeta {
    file_id: FileId,
    start_key: Key,
    last_key: Key,
}

pub fn apply_level_change(sstable_file_meta: &Vec<SStableFileMeta>) -> Vec<SStableFileMeta> {
    todo!()
}

impl Level {
    // sstables is in order
    pub fn new(sstable_metas: Vec<SStableFileMeta>, home_path: PathBuf, cache: ThreadSafeSSTableMetaCache, file_manager: ThreadSafeFileManager) -> Self {
        Level { sstable_file_metas: sstable_metas, sstable_cache: cache, home_path, file_manager }
    }
    pub fn get_in_level_0(&self, key: &Key) -> Result<Option<Value>> {
        assert!(!self.sstable_file_metas.is_empty());

        if self.last_key().le(key) {
            return Ok(None);
        }

        for meta in &self.sstable_file_metas {
            let sstable = self.get_sstable(meta)?;
            let res = sstable.get(key)?;
            if let Some(v) = res {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }
    pub fn get(&self, key: &Key) -> Result<Option<Value>> {
        assert!(!self.sstable_file_metas.is_empty());

        if self.last_key().le(key) {
            return Ok(None);
        }
        // binary search sstable which key range contains key
        let position = self.sstable_file_metas.partition_point(|meta| {
            meta.last_key().lt(key)
        });
        // find in sstable
        let sstable_file_meta: &SStableFileMeta = self.sstable_file_metas.get(position).expect("must find");
        let sstable = self.get_sstable(&sstable_file_meta)?;
        sstable.get(key)
    }

    fn get_sstable(&self, sstable_file_meta: &SStableFileMeta) -> Result<SSTable> {
        let file_id = sstable_file_meta.file_id();
        let sstable_file_meta = self.get_sstable_meta(&file_id)?;
        let file = File::open(FileStorageManager::file_path(self.home_path.as_path(), &file_id))?;
        let sstable = SSTable::from(sstable_file_meta, file)?;
        Ok(sstable)
    }

    fn get_sstable_meta(&self, file_id: &FileId) -> Result<Arc<SStableBlockMeta>> {
        let mut cache = self.sstable_cache.lock().unwrap();
        let res = cache.get(file_id);
        return if res.is_none() {
            let path = FileStorageManager::file_path(self.home_path.as_path(), file_id);
            let mut file = File::open(&path)?;
            let sstable_meta = Arc::new(SSTable::get_meta_from_file(&mut file)?);
            cache.push(*file_id, sstable_meta.clone());
            Ok(sstable_meta)
        } else {
            Ok(res.unwrap().clone())
        };
    }
    fn last_key(&self) -> Key {
        let sstable_meta = self.sstable_file_metas.last().expect("sstable ids wouldn't be empty");
        sstable_meta.last_key.clone()
    }
    // find all sstable which key range has overlap in [start_key,end_key]
    fn key_overlap(&self, start_key: &Key, end_key: &Key) -> Vec<SStableFileMeta> {
        let last_key = self.last_key();
        if last_key.lt(&start_key) {
            return Vec::new();
        }
        // find first sstable which last key is greater or equal to start_key as first sstable
        let start = self.sstable_file_metas.partition_point(|sstable_meta| sstable_meta.last_key().lt(&start_key));
        // find last sstable which last key is greater or equal to end_key as end sstable
        if last_key.le(&end_key) {
            return Vec::from(&self.sstable_file_metas[start..]);
        }
        let end = self.sstable_file_metas.partition_point(|sstable_meta| sstable_meta.last_key().lt(&end_key));
        return Vec::from(&self.sstable_file_metas[start..end + 1]);
    }

    pub fn write_memtable_to_sstable_file(memtable: &Memtable, file_manager: &mut FileStorageManager) -> Result<Vec<SStableFileMeta>> {
        let mut iter = memtable.iter();
        let mut res = Vec::new();
        loop {
            let (mut file, file_id, _) = file_manager.new_file()?;
            let sstable = SSTable::build(&mut iter, file)?;
            res.push(SStableFileMeta::from(&sstable, file_id));
            if !iter.has_next() {
                break;
            }
        }
        Ok(res)
    }

    // compact n-1 level sstable to this level, build new sstable, return all sstable file id after compact, level is unchanged in compact
    // todo handle empty level
    // todo return level change
    pub fn compact_sstable(&self, mut input_sstables_metas: Vec<SStableFileMeta>) -> Result<Vec<SStableFileMeta>> {
        let start_key: Key = input_sstables_metas.iter().map(|sstable| sstable.start_key()).min().unwrap();
        let end_key: Key = input_sstables_metas.iter().map(|sstable| sstable.last_key()).max().unwrap();
        // find key overlap sstable
        let mut sstable_overlap = self.key_overlap(&start_key, &end_key);
        input_sstables_metas.append(&mut sstable_overlap);

        let mut input_sstables = Vec::new();
        for sstable_file_meta in input_sstables_metas {
            let sstable_block_metas = self.get_sstable_meta(&sstable_file_meta.file_id())?;
            let file = FileStorageManager::open_file(self.home_path.as_path(), &sstable_file_meta.file_id())?;
            let sstable = SSTable::from(sstable_block_metas, file)?;
            input_sstables.push(sstable);
        }

        let mut input_sstables_iter = Vec::new();
        for sstable in &input_sstables {
            let iter = sstable.iter()?;
            input_sstables_iter.push(iter)
        }

        let mut input_sstable_iter_ref: Vec<&mut SStableIter> = input_sstables_iter.iter_mut().collect();
        let mut sstable_iters: Vec<&mut dyn Iterator<Item=(KeySlice, ValueSliceTag)>> = Vec::new();
        input_sstable_iter_ref.reverse();
        while !input_sstable_iter_ref.is_empty() {
            sstable_iters.push(input_sstable_iter_ref.pop().unwrap());
        }

        // build new sstable, write to stable_writer
        let mut sorted_iter = SortedKVIter::new(sstable_iters);
        let mut res = Vec::new();
        loop {
            let (mut file, file_id, _) = self.file_manager.lock().unwrap().new_file()?;
            let sstable = SSTable::build(&mut sorted_iter, file)?;
            let meta = SStableFileMeta::from(&sstable, file_id);
            res.push(meta);
            if !sorted_iter.has_next() {
                break;
            }
        }
        Ok(res)
    }

    pub fn new_cache(capacity: usize) -> ThreadSafeSSTableMetaCache {
        Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(capacity).unwrap())))
    }

    pub fn len(&self) -> usize {
        self.sstable_file_metas.len()
    }
}

impl SStableFileMeta {
    pub fn new(start_key: Key, end_key: Key, file_id: FileId) -> Self {
        SStableFileMeta { start_key, last_key: end_key, file_id }
    }
    pub fn from(sstable: &SSTable, file_id: FileId) -> Self {
        let sstable_meta = sstable.block_metadata();
        Self::new(sstable_meta.first_key(), sstable_meta.last_key(), file_id)
    }
    pub fn start_key(&self) -> Key {
        self.start_key.clone()
    }
    pub fn last_key(&self) -> Key {
        self.last_key.clone()
    }
    pub fn file_id(&self) -> FileId {
        self.file_id
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::num::NonZeroUsize;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use lru::LruCache;
    use tempfile::tempdir;

    use crate::db::file_storage::FileStorageManager;
    use crate::db::key::Key;
    use crate::db::level::{Level, SStableFileMeta};
    use crate::db::memtable::Memtable;
    use crate::db::sstable::SSTable;
    use crate::db::sstable::test::{build_sstable, build_sstable_with_special_value};
    use crate::db::value::{Value, ValueSlice};

    fn build_level() -> Level {
        let dir = tempdir().unwrap();
        let path = dir.into_path();
        let mut file_manager = FileStorageManager::new(path.clone());
        let a_file = file_manager.new_file().unwrap().0;
        let a = build_sstable(100, 200, 1, a_file);
        let a_meta = SStableFileMeta::new(a.start_key().clone(), a.last_key().clone(), 0);
        // println!("{:?}", a.get(&Key::new("56")).unwrap());
        let b_file = file_manager.new_file().unwrap().0;
        let b = build_sstable(205, 300, 1, b_file);
        let b_meta = SStableFileMeta::new(b.start_key().clone(), b.last_key().clone(), 1);
        let c_file = file_manager.new_file().unwrap().0;
        let c = build_sstable(305, 400, 1, c_file);
        let c_meta = SStableFileMeta::new(c.start_key().clone(), c.last_key().clone(), 2);
        // [100-200),[205-300),[305-400)
        Level::new(vec![a_meta, b_meta, c_meta], path, Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(10).unwrap()))), Arc::new(Mutex::new(file_manager)))
    }

    #[test]
    fn test_get_in_level_0() {
        // a:[11,20] b[15,25) c[26,30)
        // set 16 to "a" in a
        let dir = tempdir().unwrap();
        let path = dir.into_path();
        let mut file_manager = FileStorageManager::new(path.clone());
        let a_file = file_manager.new_file().unwrap().0;
        let mut map = HashMap::new();

        map.insert(16, Some(Value::new("a")));

        let a = build_sstable_with_special_value(11, 20, 1, map, a_file);
        let a_meta = SStableFileMeta::new(a.start_key().clone(), a.last_key().clone(), 0);
        // println!("{:?}", a.get(&Key::new("56")).unwrap());
        let b_file = file_manager.new_file().unwrap().0;
        let b = build_sstable(15, 25, 1, b_file);
        let b_meta = SStableFileMeta::new(b.start_key().clone(), b.last_key().clone(), 1);
        let c_file = file_manager.new_file().unwrap().0;
        let c = build_sstable(26, 30, 1, c_file);
        let c_meta = SStableFileMeta::new(c.start_key().clone(), c.last_key().clone(), 2);

        let level = Level::new(vec![a_meta, b_meta, c_meta], path, Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(10).unwrap()))), Arc::new(Mutex::new(file_manager)));

        let res = level.get_in_level_0(&Key::new("12")).unwrap();
        assert_eq!(res, Some(Value::new("12")));

        let res = level.get_in_level_0(&Key::new("16")).unwrap();
        assert_eq!(res, Some(Value::new("a")));

        let res = level.get_in_level_0(&Key::new("19")).unwrap();
        assert_eq!(res, Some(Value::new("19")));

        let res = level.get_in_level_0(&Key::new("1")).unwrap();
        assert!(res.is_none());
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
        assert_eq!(res.get(0).unwrap().last_key(), Key::new("199"));

        let res = level.key_overlap(&Key::new("450"), &Key::new("480"));
        assert_eq!(res.len(), 0);

        let res = level.key_overlap(&Key::new("120"), &Key::new("280"));
        assert_eq!(res.len(), 2);
        assert_eq!(res.get(1).unwrap().last_key(), Key::new("299"));

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
        let dir = tempdir().unwrap();
        let home_path = PathBuf::from(dir.path());
        let mut file_manager = FileStorageManager::new_thread_safe_manager(dir.into_path());

        // a [100,110) delete 109 set 105 to X b[108,115] set 113 to Z set 109 to Z
        // c [105,108) ,d [110,115) set 112 to Y, e[122,124)
        // sstable create order a>b>c....>e, so priority order is a>b>...e
        let mut special_value_map = HashMap::new();
        special_value_map.insert(109, None);
        special_value_map.insert(105, Some(Value::new("X")));

        let (mut a_file, a_file_id, _) = file_manager.lock().unwrap().new_file().unwrap();
        let a = build_sstable_with_special_value(100, 110, 1, special_value_map, a_file);
        let a_file_meta = SStableFileMeta::from(&a, a_file_id);

        let mut special_value_map = HashMap::new();
        special_value_map.insert(109, Some(Value::new("Z")));
        special_value_map.insert(113, Some(Value::new("Z")));
        let (mut b_file, b_file_id, _) = file_manager.lock().unwrap().new_file().unwrap();
        let b = build_sstable_with_special_value(108, 115, 1, special_value_map, b_file);
        let b_file_meta = SStableFileMeta::from(&b, b_file_id);

        let (mut c_file, c_file_id, _) = file_manager.lock().unwrap().new_file().unwrap();
        let c = build_sstable(105, 108, 1, c_file);
        let c_file_meta = SStableFileMeta::from(&c, c_file_id);

        let mut special_value_map = HashMap::new();
        special_value_map.insert(112, Some(Value::new("Y")));
        let (mut d_file, d_file_id, _) = file_manager.lock().unwrap().new_file().unwrap();
        let d = build_sstable_with_special_value(110, 115, 1, special_value_map, d_file);
        let d_file_meta = SStableFileMeta::from(&d, d_file_id);

        let (mut e_file, e_file_id, _) = file_manager.lock().unwrap().new_file().unwrap();
        let e = build_sstable(122, 124, 1, e_file);
        let e_file_meta = SStableFileMeta::from(&e, e_file_id);

        let mut level = Level::new(vec![c_file_meta, d_file_meta, e_file_meta], home_path.clone(), Level::new_cache(10), file_manager);

        let mut file_sstable = level.compact_sstable(vec![a_file_meta, b_file_meta]).unwrap();
        assert_eq!(file_sstable.len(), 1);
        let file_id = file_sstable.pop().unwrap().file_id;
        let mut file = FileStorageManager::open_file(&home_path, &file_id).unwrap();
        let sstable = SSTable::from_file(file).unwrap();
        let expect = "(key: 100,value: 100)(key: 101,value: 101)(key: 102,value: 102)(key: 103,value: 103)(key: 104,value: 104)(key: 105,value: X)(key: 106,value: 106)(key: 107,value: 107)(key: 108,value: 108)(key: 109,value: None)(key: 110,value: 110)(key: 111,value: 111)(key: 112,value: 112)(key: 113,value: Z)(key: 114,value: 114)";
        assert_eq!(sstable.to_string(), expect);
    }

    #[test]
    fn test_write_memtable_to_sstable() {
        let mut memtable = Memtable::new();
        for i in 0..10 {
            memtable.insert(&Key::from(i.to_string().as_bytes()), &Value::new(&i.to_string()));
        }
        let dir = tempdir().unwrap();
        let home_path = dir.path();
        let mut file_manager = FileStorageManager::new(PathBuf::from(home_path));
        let mut sstables = Level::write_memtable_to_sstable_file(&memtable, &mut file_manager).unwrap();
        assert_eq!(sstables.len(), 1);
        let meta = sstables.pop().unwrap();
        let file = FileStorageManager::open_file(home_path, &meta.file_id).unwrap();
        let sstable = SSTable::from_file(file).unwrap();

        assert_eq!(format!("{:}", sstable), "(key: 0,value: 0)(key: 1,value: 1)(key: 2,value: 2)(key: 3,value: 3)(key: 4,value: 4)(key: 5,value: 5)(key: 6,value: 6)(key: 7,value: 7)(key: 8,value: 8)(key: 9,value: 9)");
    }
}
