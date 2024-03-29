use std::cell::RefCell;
use std::collections::HashSet;
use std::fs::File;
use std::io::Write;
use std::num::NonZeroUsize;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use log::debug;
use lru::LruCache;
use serde::{Deserialize, Serialize};

use crate::db::common::{KVIterItem, SortedKVIter, ValueSliceTag};
use crate::db::file_storage::{FileId, FileStorageManager, ThreadSafeFileManager};
use crate::db::key::{Key, KeySlice};
use crate::db::memtable::Memtable;
use crate::db::sstable::{SSTable, SStableBlockMeta, SStableIter};
use crate::db::value::{Value, ValueSlice};

use super::common::ValueWithTag;

pub type SSTableBlockMetaCache = LruCache<FileId, Arc<SStableBlockMeta>>;
pub type ThreadSafeSSTableMetaCache = Arc<Mutex<SSTableBlockMetaCache>>;

// immutable, own by version
pub struct Level {
    sstable_cache: ThreadSafeSSTableMetaCache,
    sstable_file_metas: Vec<SStableFileMeta>,
    file_manager: ThreadSafeFileManager,
    home_path: PathBuf,
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub enum LevelChange {
    // add new sstable to level start from position_in_level,sstable order is same as sstable_file_metas
    MemtableCompact {
        sstable_file_metas: SStableFileMeta,
    },
    LevelCompact {
        // compact 1 to 2, compact_from_leve is 1
        compact_from_level: usize,
        compact_sstable: SStableFileMeta,
        compact_result: CompactSStableResult,
    },
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct SStableFileMeta {
    file_id: FileId,
    start_key: Key,
    last_key: Key,
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct CompactSStableResult {
    pub remove_sstables: Vec<SStableFileMeta>,
    pub add_sstables: Vec<SStableFileMeta>,
    pub position: usize,
}

impl Level {
    // sstables is in order
    pub fn new(
        sstable_metas: Vec<SStableFileMeta>,
        home_path: PathBuf,
        cache: ThreadSafeSSTableMetaCache,
        file_manager: ThreadSafeFileManager,
    ) -> Self {
        Level {
            sstable_file_metas: sstable_metas,
            sstable_cache: cache,
            home_path,
            file_manager,
        }
    }
    pub fn get_in_level_0(&self, key: &Key) -> Result<Option<ValueWithTag>> {
        assert!(!self.sstable_file_metas.is_empty());

        for meta in &self.sstable_file_metas {
            let sstable = self.get_sstable(meta)?;
            let res = sstable.get(key)?;
            if let Some(v) = res {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }
    pub fn get(&self, key: &Key) -> Result<Option<ValueWithTag>> {
        assert!(!self.sstable_file_metas.is_empty());

        if self.last_key().lt(key) {
            return Ok(None);
        }
        // binary search sstable which key range contains key
        let position = self
            .sstable_file_metas
            .partition_point(|meta| meta.last_key().lt(key));
        // find in sstable
        let sstable_file_meta: &SStableFileMeta =
            self.sstable_file_metas.get(position).expect("must find");
        let sstable = self.get_sstable(&sstable_file_meta)?;
        sstable.get(key)
    }

    fn get_sstable(&self, sstable_file_meta: &SStableFileMeta) -> Result<SSTable> {
        let file_id = sstable_file_meta.file_id();
        let sstable_file_meta = self.get_sstable_meta(&file_id)?;
        let file = File::open(FileStorageManager::file_path(
            self.home_path.as_path(),
            &file_id,
        ))?;
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
        let sstable_meta = self
            .sstable_file_metas
            .last()
            .expect("sstable ids wouldn't be empty");
        sstable_meta.last_key.clone()
    }
    fn first_key(&self) -> Key {
        let sstable_meta = self
            .sstable_file_metas
            .first()
            .expect("sstable ids wouldn't be empty");
        sstable_meta.start_key.clone()
    }
    // find all sstable which key range has overlap in [start_key,end_key]
    // return first overlaps sstable position
    fn key_overlap(&self, start_key: &Key, end_key: &Key) -> Option<(Vec<SStableFileMeta>, usize)> {
        let last_key = self.last_key();
        if last_key.lt(&start_key) {
            return None;
        }
        if self.first_key().gt(&end_key) {
            return None;
        }
        // find first sstable which last key is greater or equal to start_key as first sstable
        let start = self
            .sstable_file_metas
            .partition_point(|sstable_meta| sstable_meta.last_key().lt(&start_key));
        // find last sstable which last key is greater or equal to end_key as end sstable
        if last_key.le(&end_key) {
            return Some((Vec::from(&self.sstable_file_metas[start..]), start));
        }
        let end = self
            .sstable_file_metas
            .partition_point(|sstable_meta| sstable_meta.last_key().lt(&end_key));
        return Some((Vec::from(&self.sstable_file_metas[start..end + 1]), start));
    }

    pub fn write_memtable_to_sstable_file(
        memtable: &Memtable,
        file_manager: &mut FileStorageManager,
    ) -> Result<Vec<SStableFileMeta>> {
        let mut iter = memtable.iter();
        let mut res = Vec::new();
        loop {
            let (file, file_id, _) = file_manager.new_file()?;
            let (sstable_opt, has_next) = SSTable::from_iter(&mut iter, file)?;
            if sstable_opt.is_none() {
                break;
            }
            let sstable = sstable_opt.unwrap();
            res.push(SStableFileMeta::from(&sstable, file_id));
            if !has_next {
                break;
            }
        }
        Ok(res)
    }

    // compact n-1 level sstable to this level, build new sstable,
    // level is unchanged in compact
    // return (new_sstable in current level ,remove_sstable  start_position in current level)
    pub fn compact_sstable(
        &self,
        mut input_sstables_metas: Vec<SStableFileMeta>,
        discard_deleted_kv: bool,
    ) -> Result<CompactSStableResult> {
        let start_key: Key = input_sstables_metas
            .iter()
            .map(|sstable| sstable.start_key())
            .min()
            .unwrap();
        let end_key: Key = input_sstables_metas
            .iter()
            .map(|sstable| sstable.last_key())
            .max()
            .unwrap();
        // find key overlap sstable
        let key_overlap_res = self.key_overlap(&start_key, &end_key);
        if key_overlap_res.is_none() {
            let position;
            if self.last_key().lt(&start_key) {
                position = self.len();
            } else {
                position = 0;
            }
            return Ok(CompactSStableResult {
                remove_sstables: vec![],
                add_sstables: input_sstables_metas,
                position,
            });
        }
        let (sstable_overlap, start_position) = key_overlap_res.unwrap();
        input_sstables_metas.append(&mut sstable_overlap.clone());

        let mut input_sstables = Vec::new();
        for sstable_file_meta in input_sstables_metas {
            let sstable_block_metas = self.get_sstable_meta(&sstable_file_meta.file_id())?;
            let file = FileStorageManager::open_file(
                self.home_path.as_path(),
                &sstable_file_meta.file_id(),
            )?;
            let sstable = SSTable::from(sstable_block_metas, file)?;
            input_sstables.push(sstable);
        }

        let mut input_sstables_iter = Vec::new();
        for sstable in &input_sstables {
            let iter = sstable.iter()?;
            input_sstables_iter.push(iter)
        }

        let mut input_sstable_iter_ref: Vec<&mut SStableIter> =
            input_sstables_iter.iter_mut().collect();
        let mut sstable_iters: Vec<&mut dyn Iterator<Item = (KeySlice, ValueSliceTag)>> =
            Vec::new();
        input_sstable_iter_ref.reverse();
        while !input_sstable_iter_ref.is_empty() {
            sstable_iters.push(input_sstable_iter_ref.pop().unwrap());
        }

        // build new sstable, write to stable_writer
        let mut sorted_iter = SortedKVIter::new(sstable_iters);
        let mut res = Vec::new();
        loop {
            let (file, file_id, _) = self.file_manager.lock().unwrap().new_file()?;
            let (sstable_opt, has_next) =
                build_sstable_from_iters(&mut sorted_iter, file, discard_deleted_kv)?;
            if sstable_opt.is_none() {
                break;
            }
            let sstable = sstable_opt.unwrap();
            let meta = SStableFileMeta::from(&sstable, file_id);
            res.push(meta);
            if !has_next {
                break;
            }
        }
        Ok(CompactSStableResult {
            remove_sstables: sstable_overlap,
            add_sstables: res,
            position: start_position,
        })
    }

    pub fn copy_sstable_meta(&self) -> Vec<SStableFileMeta> {
        self.sstable_file_metas.clone()
    }

    pub fn new_cache(capacity: usize) -> ThreadSafeSSTableMetaCache {
        Arc::new(Mutex::new(LruCache::new(
            NonZeroUsize::new(capacity).unwrap(),
        )))
    }

    pub fn len(&self) -> usize {
        self.sstable_file_metas.len()
    }

    pub fn pick_file_to_compact(&self) -> &SStableFileMeta {
        self.find_oldest_sstable()
    }

    // for test
    pub fn get_kvs_for_test(&self) -> Vec<(Key, Option<Value>)> {
        let mut res = Vec::new();
        for meta in &self.sstable_file_metas {
            let sstable = self.get_sstable(&meta).unwrap();
            let iter = sstable.iter().unwrap();
            for (k, v) in iter {
                unsafe {
                    let value = if let Some(v_data) = v {
                        Some(Value::from_u8(v_data.data()))
                    } else {
                        None
                    };
                    res.push((Key::from(k.data()), value))
                }
            }
        }
        res
    }

    pub fn get_all_file_id(&self) -> HashSet<FileId> {
        let mut res = HashSet::new();
        for meta in self.sstable_file_metas.iter() {
            res.insert(meta.file_id());
        }
        res
    }

    fn find_oldest_sstable(&self) -> &SStableFileMeta {
        let res = self
            .sstable_file_metas
            .iter()
            .min_by(|a, b| a.file_id.cmp(&b.file_id))
            .unwrap();
        res
    }
}

fn build_sstable_from_iters(
    sorted_iter: &mut SortedKVIter,
    file: File,
    discard_deleted_kv: bool,
) -> Result<(Option<SSTable>, bool), anyhow::Error> {
    if discard_deleted_kv {
        let mut prune_deleted_kv_iter = sorted_iter.filter(|kv| kv.1.is_some());
        let (sstable_opt, has_next) = SSTable::from_iter(&mut prune_deleted_kv_iter, file)?;
        Ok((sstable_opt, has_next))
    } else {
        let (sstable_opt, has_next) = SSTable::from_iter(sorted_iter, file)?;
        Ok((sstable_opt, has_next))
    }
}

impl SStableFileMeta {
    pub fn new(start_key: Key, end_key: Key, file_id: FileId) -> Self {
        SStableFileMeta {
            start_key,
            last_key: end_key,
            file_id,
        }
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
    use crate::db::sstable::test::{build_sstable, build_sstable_with_special_value};
    use crate::db::sstable::SSTable;
    use crate::db::value::{Value, ValueSlice};

    fn build_level() -> Level {
        let dir = tempdir().unwrap();
        let path = dir.into_path();
        let mut file_manager = FileStorageManager::new(&path);
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
        Level::new(
            vec![a_meta, b_meta, c_meta],
            path,
            Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(10).unwrap()))),
            Arc::new(Mutex::new(file_manager)),
        )
    }

    #[test]
    fn test_get_in_level_0() {
        // a:[11,20) b[15,25) c[26,30)
        // set 16 to "a" in a
        let dir = tempdir().unwrap();
        let path = dir.into_path();
        let mut file_manager = FileStorageManager::new(&path);
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

        let level = Level::new(
            vec![a_meta, b_meta, c_meta],
            path,
            Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(10).unwrap()))),
            Arc::new(Mutex::new(file_manager)),
        );

        let res = level.get_in_level_0(&Key::new("12")).unwrap();
        assert_eq!(res, Some(Some(Value::new("12"))));

        let res = level.get_in_level_0(&Key::new("16")).unwrap();
        assert_eq!(res, Some(Some(Value::new("a"))));

        let res = level.get_in_level_0(&Key::new("19")).unwrap();
        assert_eq!(res, Some(Some(Value::new("19"))));

        let res = level.get_in_level_0(&Key::new("29")).unwrap();
        assert_eq!(res, Some(Some(Value::new("29"))));

        let res = level.get_in_level_0(&Key::new("1")).unwrap();
        assert!(res.is_none());
    }

    #[test]
    fn test_get() {
        let level = build_level();
        assert_eq!(
            Value::new("126"),
            level.get(&Key::new("126")).unwrap().unwrap().unwrap()
        );
        assert_eq!(
            Value::new("226"),
            level.get(&Key::new("226")).unwrap().unwrap().unwrap()
        );
        assert_eq!(
            Value::new("399"),
            level.get(&Key::new("399")).unwrap().unwrap().unwrap()
        );
        assert_eq!(
            Value::new("305"),
            level.get(&Key::new("305")).unwrap().unwrap().unwrap()
        );
        assert!(level.get(&Key::new("526")).unwrap().is_none());
        assert!(level.get(&Key::new("303")).unwrap().is_none());
        assert!(level.get(&Key::new("304")).unwrap().is_none());
        assert!(level.get(&Key::new("400")).unwrap().is_none());
    }

    #[test]
    fn test_find_oldest_sstable() {
        let level = build_level();
        let res = level.find_oldest_sstable();
        assert_eq!(res.file_id, 0);
        assert_eq!(res.start_key, Key::new("100"));
        let res = level.pick_file_to_compact();
        assert_eq!(res.start_key(), Key::new("100"));
    }

    #[test]
    fn test_key_overlap() {
        // [100-200),[205-300),[305-400)
        let level = build_level();
        let res = level.key_overlap(&Key::new("050"), &Key::new("080"));
        assert!(res.is_none());

        let res = level
            .key_overlap(&Key::new("050"), &Key::new("100"))
            .unwrap()
            .0;
        assert_eq!(res.len(), 1);
        assert_eq!(res.get(0).unwrap().last_key(), Key::new("199"));

        let res = level
            .key_overlap(&Key::new("399"), &Key::new("480"))
            .unwrap()
            .0;
        assert_eq!(res.len(), 1);
        assert_eq!(res.get(0).unwrap().last_key(), Key::new("399"));

        let res = level.key_overlap(&Key::new("450"), &Key::new("480"));
        assert!(res.is_none());

        let res = level
            .key_overlap(&Key::new("120"), &Key::new("280"))
            .unwrap();
        let metas = res.0;
        assert_eq!(metas.len(), 2);
        assert_eq!(metas.get(1).unwrap().last_key(), Key::new("299"));
        assert_eq!(res.1, 0);

        let res = level
            .key_overlap(&Key::new("090"), &Key::new("380"))
            .unwrap()
            .0;
        assert_eq!(res.len(), 3);

        let res = level
            .key_overlap(&Key::new("199"), &Key::new("280"))
            .unwrap()
            .0;
        assert_eq!(res.len(), 2);

        let res = level
            .key_overlap(&Key::new("200"), &Key::new("305"))
            .unwrap();
        assert_eq!(res.0.len(), 2);
        assert_eq!(res.1, 1);

        let res = level
            .key_overlap(&Key::new("199"), &Key::new("305"))
            .unwrap()
            .0;
        assert_eq!(res.len(), 3);
    }

    #[test]
    fn test_compact() {
        let dir = tempdir().unwrap();
        let home_path = PathBuf::from(dir.path());
        let file_manager = FileStorageManager::new_thread_safe_manager(dir.into_path());

        // a [100,110) delete 109 set 105 to X b[108,115] set 113 to Z set 109 to Z
        // c [105,108) ,d [110,115) set 112 to Y, e[122,124)
        // sstable create order a>b>c....>e, so priority order is a>b>...e
        let mut special_value_map = HashMap::new();
        special_value_map.insert(109, None);
        special_value_map.insert(105, Some(Value::new("X")));

        let (a_file, a_file_id, _) = file_manager.lock().unwrap().new_file().unwrap();
        let a = build_sstable_with_special_value(100, 110, 1, special_value_map, a_file);
        let a_file_meta = SStableFileMeta::from(&a, a_file_id);

        let mut special_value_map = HashMap::new();
        special_value_map.insert(109, Some(Value::new("Z")));
        special_value_map.insert(113, Some(Value::new("Z")));
        let (b_file, b_file_id, _) = file_manager.lock().unwrap().new_file().unwrap();
        let b = build_sstable_with_special_value(108, 115, 1, special_value_map, b_file);
        let b_file_meta = SStableFileMeta::from(&b, b_file_id);

        let (c_file, c_file_id, _) = file_manager.lock().unwrap().new_file().unwrap();
        let c = build_sstable(105, 108, 1, c_file);
        let c_file_meta = SStableFileMeta::from(&c, c_file_id);

        let mut special_value_map = HashMap::new();
        special_value_map.insert(112, Some(Value::new("Y")));
        let (d_file, d_file_id, _) = file_manager.lock().unwrap().new_file().unwrap();
        let d = build_sstable_with_special_value(110, 115, 1, special_value_map, d_file);
        let d_file_meta = SStableFileMeta::from(&d, d_file_id);

        let (e_file, e_file_id, _) = file_manager.lock().unwrap().new_file().unwrap();
        let e = build_sstable(122, 124, 1, e_file);
        let e_file_meta = SStableFileMeta::from(&e, e_file_id);

        let level = Level::new(
            vec![c_file_meta, d_file_meta, e_file_meta],
            home_path.clone(),
            Level::new_cache(10),
            file_manager,
        );

        let mut file_sstable = level
            .compact_sstable(vec![a_file_meta, b_file_meta], false)
            .unwrap()
            .add_sstables;
        assert_eq!(file_sstable.len(), 1);
        let file_id = file_sstable.pop().unwrap().file_id;
        let file = FileStorageManager::open_file(&home_path, &file_id).unwrap();
        let sstable = SSTable::from_file(file).unwrap();
        let expect = "(key: 100,value: 100)(key: 101,value: 101)(key: 102,value: 102)(key: 103,value: 103)(key: 104,value: 104)(key: 105,value: X)(key: 106,value: 106)(key: 107,value: 107)(key: 108,value: 108)(key: 109,value: None)(key: 110,value: 110)(key: 111,value: 111)(key: 112,value: 112)(key: 113,value: Z)(key: 114,value: 114)";
        assert_eq!(sstable.to_string(), expect);
    }

    #[test]
    fn test_write_memtable_to_sstable() {
        let memtable = Memtable::new();
        for i in 0..10 {
            memtable.insert(
                &Key::from(i.to_string().as_bytes()),
                &Value::new(&i.to_string()),
            );
        }
        let dir = tempdir().unwrap();
        let home_path = dir.path();
        let mut file_manager = FileStorageManager::new(home_path);
        let mut sstables =
            Level::write_memtable_to_sstable_file(&memtable, &mut file_manager).unwrap();
        assert_eq!(sstables.len(), 1);
        let meta = sstables.pop().unwrap();
        let file = FileStorageManager::open_file(home_path, &meta.file_id).unwrap();
        let sstable = SSTable::from_file(file).unwrap();

        assert_eq!(format!("{:}", sstable), "(key: 0,value: 0)(key: 1,value: 1)(key: 2,value: 2)(key: 3,value: 3)(key: 4,value: 4)(key: 5,value: 5)(key: 6,value: 6)(key: 7,value: 7)(key: 8,value: 8)(key: 9,value: 9)");
    }
    #[test]
    fn test_all_file_id() {
        let level = build_level();
        let ids=level.get_all_file_id();
        assert!(ids.contains(&0));
        assert!(ids.contains(&1));
        assert!(ids.contains(&2));

        assert_eq!(ids.len(), 3);
    }
}
