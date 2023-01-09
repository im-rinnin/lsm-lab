use metrics::histogram;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use log::info;

use crate::db::config;
use crate::db::config::Config;
use crate::db::db_metrics::READ_HIT_SSTABLE_LEVEL;
use crate::db::file_storage::{FileId, FileStorageManager, ThreadSafeFileManager};
use crate::db::key::Key;
use crate::db::level::{
    CompactSStableResult, Level, LevelChange, SStableFileMeta, ThreadSafeSSTableMetaCache,
};
use crate::db::memtable::Memtable;
use crate::db::meta_log::{MetaLog, MetaLogIter};
use crate::db::sstable::SSTable;
use crate::db::value::Value;

use super::common::ValueWithTag;
use super::db_metrics::{DBMetric, TimeRecorder, SSTABLE_COMPACT_TIME};

// all sstable meta
// immutable, thread safe,create new version after insert new sstable/compact
pub struct Version {
    // all level info,order by level number,vec[0]->level 0
    levels: HashMap<usize, Level>,
    sstable_cache: ThreadSafeSSTableMetaCache,
    file_manager: ThreadSafeFileManager,
    home_path: PathBuf,
    config: Config,
}

impl Version {
    pub fn new(
        home_path: &Path,
        file_manager: ThreadSafeFileManager,
        sstable_cache: ThreadSafeSSTableMetaCache,
    ) -> Self {
        Version {
            levels: HashMap::new(),
            sstable_cache,
            file_manager,
            home_path: PathBuf::from(home_path),
            config: Config::new(),
        }
    }
    pub fn from(
        level_change_iter: &mut dyn Iterator<Item = LevelChange>,
        home_path: PathBuf,
        file_manager: ThreadSafeFileManager,
        sstable_cache: ThreadSafeSSTableMetaCache,
    ) -> Result<Self> {
        // iter meta log,get level change
        let mut level_sstable_file_metas: HashMap<usize, Vec<SStableFileMeta>> = HashMap::new();
        for level_change in level_change_iter {
            // let level_change: LevelChange = serde_json::from_slice(data?.as_slice())?;
            Version::apply_level_change(&mut level_sstable_file_metas, level_change)
        }
        let mut levels = HashMap::new();
        Version::build_level(
            &home_path,
            &file_manager,
            &sstable_cache,
            &mut level_sstable_file_metas,
            &mut levels,
        );
        Ok(Version {
            levels,
            sstable_cache,
            file_manager,
            home_path,
            config: Config::new(),
        })
    }

    // pick and find one level to compact
    pub fn compact_one_level(&self) -> Result<Option<LevelChange>> {
        // pick one level from 0 to n
        let depth = self.depth();
        for level_number in 0..depth {
            let level_option = self.levels.get(&level_number);
            if level_option.is_none() {
                continue;
            }
            let level = level_option.unwrap();
            let len = level.len();
            if len > Self::level_file_number_limit(level_number, &self.config) {
                info!("start compact level {}", level_number);
                let res = self.do_compact(level_number, level);
                info!("{} compact finished", level_number);
                return res;
            }
        }
        return Ok(None);
    }

    fn do_compact(&self, level_number: usize, level: &Level) -> Result<Option<LevelChange>> {
        let recorder = TimeRecorder::new(SSTABLE_COMPACT_TIME);

        let sstable_for_compact = level.pick_file_to_compact();
        let next_level_option = self.levels.get(&(level_number + 1));
        if next_level_option.is_none() {
            let level_change = LevelChange::LevelCompact {
                compact_from_level: level_number,
                compact_sstable: sstable_for_compact.clone(),
                compact_result: CompactSStableResult {
                    remove_sstables: vec![],
                    add_sstables: vec![sstable_for_compact.clone()],
                    position: 0,
                },
            };
            return Ok(Some(level_change));
        }

        let next_level = next_level_option.unwrap();
        // pick files  do compact
        let compact_res = next_level.compact_sstable(vec![sstable_for_compact.clone()])?;
        let level_change = LevelChange::LevelCompact {
            compact_from_level: level_number,
            compact_sstable: sstable_for_compact.clone(),
            compact_result: compact_res,
        };
        return Ok(Some(level_change));
    }

    pub fn get_str(&self, key: &str) -> Result<Option<Value>> {
        self.get(&Key::new(key))
    }
    pub fn get(&self, key: &Key) -> Result<Option<Value>> {
        // call get key from level 0 to level n
        if self.depth() == 0 {
            return Ok(None);
        }
        let level_0 = self.levels.get(&0).unwrap();
        let res = level_0.get_in_level_0(key)?;
        if let Some(taged_value) = res {
            histogram!(READ_HIT_SSTABLE_LEVEL, 0.0);
            if let Some(v) = &taged_value {
                return Ok(Some(v.clone()));
            } else {
                return Ok(None);
            }
        }

        for l in 1..self.depth() {
            let level = self.levels.get(&l).unwrap();
            let res = level.get(key)?;
            if let Some(taged_value) = res {
                histogram!(READ_HIT_SSTABLE_LEVEL, l as f64);
                if let Some(v) = &taged_value {
                    return Ok(Some(v.clone()));
                } else {
                    return Ok(None);
                }
            }
        }
        Ok(None)
    }

    pub fn set_config(&mut self, config: Config) {
        self.config = config
    }

    pub fn add_memtable_to_level_0(&self, memtable: &Memtable) -> Result<LevelChange> {
        // build sstable from memtable (sstable::build)
        let mut iter = memtable.iter();
        let (file, file_id, _) = self.file_manager.lock().unwrap().new_file()?;
        let (sstable_opt, has_next) = SSTable::from_iter_with_file_limit(&mut iter, file, 0)?;
        let sstable = sstable_opt.unwrap();
        let sstable_meta = SStableFileMeta::from(&sstable, file_id);
        assert!(iter.next().is_none());
        let level_change = LevelChange::MemtableCompact {
            sstable_file_metas: sstable_meta,
        };
        Ok(level_change)
    }

    pub fn apply_change(&self, level_change: LevelChange) -> Self {
        let mut map = HashMap::new();
        for (l, level) in &self.levels {
            map.insert(*l, level.copy_sstable_meta());
        }
        Self::apply_level_change(&mut map, level_change);
        let mut levels = HashMap::new();
        Version::build_level(
            &self.home_path,
            &self.file_manager,
            &self.sstable_cache,
            &mut map,
            &mut levels,
        );
        Version {
            levels,
            sstable_cache: self.sstable_cache.clone(),
            file_manager: self.file_manager.clone(),
            home_path: self.home_path.clone(),
            config: self.config.clone(),
        }
    }

    // find max level is not empty
    pub fn depth(&self) -> usize {
        if self.levels.is_empty() {
            return 0;
        }
        let max_level = self.levels.keys().max().unwrap_or_else(|| &0);
        for l in (0..*max_level + 1).rev() {
            let level = self.levels.get(&l).unwrap();
            if level.len() != 0 {
                return l + 1;
            }
        }
        0
    }

    pub fn record_metrics(&self, metric: &DBMetric) {
        let depth = self.depth();
        for i in 0..depth {
            let level = self.levels.get(&i);
            let len = if let Some(l) = level { l.len() } else { 0 } as u64;
            metric.set_level_n_file_number(len, i);
        }
    }

    fn build_level(
        home_path: &PathBuf,
        file_manager: &ThreadSafeFileManager,
        sstable_cache: &ThreadSafeSSTableMetaCache,
        level_sstable_file_metas: &mut HashMap<usize, Vec<SStableFileMeta>>,
        levels: &mut HashMap<usize, Level>,
    ) {
        let len = level_sstable_file_metas.len();
        for i in 0..len {
            let metas = level_sstable_file_metas.remove(&i).unwrap();
            let level = Level::new(
                metas,
                home_path.clone(),
                sstable_cache.clone(),
                file_manager.clone(),
            );
            levels.insert(i, level);
        }
    }

    fn apply_level_change(
        mut level_sstable_file_metas: &mut HashMap<usize, Vec<SStableFileMeta>>,
        level_change: LevelChange,
    ) {
        match level_change {
            LevelChange::LevelCompact {
                compact_from_level,
                compact_sstable,
                compact_result,
            } => {
                // remove sstable from level
                let compact_level_metas: &mut Vec<SStableFileMeta> =
                    Self::get_or_default(&mut level_sstable_file_metas, compact_from_level);
                compact_level_metas.retain(|meta| meta.file_id().ne(&compact_sstable.file_id()));

                // remove and add sstable in next level
                let next_level_metas: &mut Vec<SStableFileMeta> =
                    Self::get_or_default(&mut level_sstable_file_metas, compact_from_level + 1);

                let remove_length = compact_result.remove_sstables.len();

                for _ in 0..remove_length {
                    next_level_metas.remove(compact_result.position);
                }
                let mut add_sstables = compact_result.add_sstables;
                while !add_sstables.is_empty() {
                    next_level_metas.insert(compact_result.position, add_sstables.pop().unwrap())
                }
            }
            LevelChange::MemtableCompact {
                sstable_file_metas: sstable_file_meta,
            } => {
                let metas: &mut Vec<SStableFileMeta> =
                    Self::get_or_default(&mut level_sstable_file_metas, 0);
                metas.insert(0, sstable_file_meta)
            }
        }
    }

    fn get_or_default(
        map: &mut HashMap<usize, Vec<SStableFileMeta>>,
        key: usize,
    ) -> &mut Vec<SStableFileMeta> {
        if map.get_mut(&key).is_some() {
            return map.get_mut(&key).unwrap();
        }
        map.insert(key, Vec::new());
        return map.get_mut(&key).unwrap();
    }

    fn level_file_number_limit(level: usize, config: &Config) -> usize {
        if level == 0 {
            return config.level_0_file_limit;
        }
        ((config.level_size_expand_factor as u32).pow(level as u32) as usize) * 1024 * 1024
            / config.sstable_file_limit
    }
}

impl Debug for Version {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let len = self.levels.len();
        for l in 0..len {
            let mut s = String::new();
            let b = self.levels.get(&l).unwrap();
            for file_meta in b.copy_sstable_meta() {
                let a = format!(
                    "file_id:{},file_start_key:{:?},file_end_key:{:?}\n",
                    file_meta.file_id(),
                    file_meta.start_key(),
                    file_meta.last_key()
                );
                s.push_str(&a)
            }
            writeln!(f, "level: {},data {}", l, s)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::num::NonZeroUsize;
    use std::sync::{Arc, Mutex};

    use anyhow::Result;
    use lru::LruCache;
    use tempfile::tempdir;

    use crate::db::config::Config;
    use crate::db::file_storage::FileStorageManager;
    use crate::db::key::Key;
    use crate::db::level::{CompactSStableResult, LevelChange, SStableFileMeta};
    use crate::db::memtable::Memtable;
    use crate::db::sstable::test::{build_sstable, build_sstable_with_special_value};
    use crate::db::value::Value;
    use crate::db::version::Version;

    fn build_level() -> Result<Version> {
        // level 0: sstable_a[12,18),sstable_b[15,20)
        // level 1:sstable_c[11,15),sstable_d[17,21)
        let dir = tempdir().unwrap();
        let mut file_manager = FileStorageManager::new(dir.path());
        let (file_c, file_c_id, _) = file_manager.new_file().unwrap();
        let (file_d, file_d_id, _) = file_manager.new_file().unwrap();
        let (file_b, file_b_id, _) = file_manager.new_file().unwrap();
        let (file_a, file_a_id, _) = file_manager.new_file().unwrap();
        let sstable_c = build_sstable(11, 15, 1, file_c);
        let c_meta = SStableFileMeta::from(&sstable_c, file_c_id);
        let sstable_d = build_sstable(17, 21, 1, file_d);
        let d_meta = SStableFileMeta::from(&sstable_d, file_d_id);

        let mut map = HashMap::new();
        map.insert(16, Some(Value::new("a")));
        let sstable_a = build_sstable_with_special_value(12, 18, 1, map, file_a);
        let a_meta = SStableFileMeta::from(&sstable_a, file_a_id);
        let mut map = HashMap::new();
        map.insert(16, Some(Value::new("b")));
        map.insert(18, Some(Value::new("b")));
        let sstable_b = build_sstable_with_special_value(15, 20, 1, map, file_b);
        let b_meta = SStableFileMeta::from(&sstable_b, file_b_id);

        let level_0_level_change_b = LevelChange::MemtableCompact {
            sstable_file_metas: b_meta,
        };
        let level_0_level_change_a = LevelChange::MemtableCompact {
            sstable_file_metas: a_meta,
        };
        let level_0_level_change_c = LevelChange::MemtableCompact {
            sstable_file_metas: c_meta.clone(),
        };
        let level_0_level_change_d = LevelChange::MemtableCompact {
            sstable_file_metas: d_meta.clone(),
        };
        let level_1_level_change_c = LevelChange::LevelCompact {
            compact_from_level: 0,
            compact_sstable: c_meta.clone(),
            compact_result: CompactSStableResult {
                remove_sstables: vec![],
                add_sstables: vec![c_meta.clone()],
                position: 0,
            },
        };
        let level_1_level_change_d = LevelChange::LevelCompact {
            compact_from_level: 0,
            compact_sstable: d_meta.clone(),
            compact_result: CompactSStableResult {
                remove_sstables: vec![],
                add_sstables: vec![d_meta.clone()],
                position: 1,
            },
        };

        let meta_log = vec![
            level_0_level_change_b,
            level_0_level_change_a,
            level_0_level_change_c,
            level_0_level_change_d,
            level_1_level_change_c,
            level_1_level_change_d,
        ];
        let mut iter = meta_log.into_iter();

        let version = Version::from(
            &mut iter,
            dir.into_path(),
            Arc::new(Mutex::new(file_manager)),
            Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(10).unwrap()))),
        )?;
        Ok(version)
    }

    #[test]
    pub fn test_build_level() {
        let version = build_level().unwrap();
        let s = "level: 0,data file_id:3,file_start_key:Key { k: \"12\" },file_end_key:Key { k: \"17\" }\nfile_id:2,file_start_key:Key { k: \"15\" },file_end_key:Key { k: \"19\" }\n\nlevel: 1,data file_id:0,file_start_key:Key { k: \"11\" },file_end_key:Key { k: \"14\" }\nfile_id:1,file_start_key:Key { k: \"17\" },file_end_key:Key { k: \"20\" }\n\n";
        assert_eq!(format!("{:?}", version), s);
    }

    #[test]
    pub fn test_depth() {
        let version = build_level().unwrap();
        assert_eq!(version.depth(), 2);

        let dir = tempdir().unwrap();
        let file_manager = FileStorageManager::new(dir.path());

        let meta_log = vec![];
        let mut iter = meta_log.into_iter();

        let empty_version = Version::from(
            &mut iter,
            dir.into_path(),
            Arc::new(Mutex::new(file_manager)),
            Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(10).unwrap()))),
        )
        .unwrap();
        assert_eq!(empty_version.depth(), 0);
    }

    #[test]
    pub fn test_add_memtable() {
        use crate::db::sstable::{SSTable, SStableBlockMeta, SStableIter};
        let version = build_level().unwrap();
        let memtable = Memtable::new();
        memtable.insert(&Key::new("12"), &Value::new("mem"));
        memtable.insert(&Key::new("7"), &Value::new("mem"));

        let level_change = version.add_memtable_to_level_0(&memtable).unwrap();

        println!("level change {:?}", level_change);
        let new_version = version.apply_change(level_change);
        println!("version {:?}", new_version);
        assert_eq!(
            version.get(&Key::new("17")).unwrap(),
            Some(Value::new("17"))
        );

        assert_eq!(
            new_version.get(&Key::new("12")).unwrap(),
            Some(Value::new("mem"))
        );
        assert_eq!(
            new_version.get(&Key::new("7")).unwrap(),
            Some(Value::new("mem"))
        );
    }

    #[test]
    pub fn test_compact_sstable() {
        let mut version_0 = build_level().unwrap();
        println!("{:?}", version_0);
        let res = version_0.get(&Key::new("18")).unwrap().unwrap();
        assert_eq!(res, Value::new("b"));
        let res = version_0.get(&Key::new("16")).unwrap().unwrap();
        assert_eq!(res, Value::new("a"));
        let mut config = Config::new();
        config.level_0_file_limit = 1;
        config.level_size_expand_factor = 1;
        version_0.set_config(config);
        let level_change = version_0.compact_one_level().unwrap().unwrap();
        let version_1 = version_0.apply_change(level_change);
        // println!("{:?}", version_1);
        assert_eq!(format!("{:?}", version_1),"level: 0,data file_id:3,file_start_key:Key { k: \"12\" },file_end_key:Key { k: \"17\" }\n\nlevel: 1,data file_id:0,file_start_key:Key { k: \"11\" },file_end_key:Key { k: \"14\" }\nfile_id:4,file_start_key:Key { k: \"15\" },file_end_key:Key { k: \"20\" }\n\n");
        let res = version_1.get(&Key::new("16")).unwrap().unwrap();
        assert_eq!(res, Value::new("a"));

        let level_change = version_1.compact_one_level().unwrap().unwrap();
        let version_2 = version_1.apply_change(level_change);
        // println!("{:?}", version_2);
        assert_eq!(format!("{:?}", version_2),"level: 0,data file_id:3,file_start_key:Key { k: \"12\" },file_end_key:Key { k: \"17\" }\n\nlevel: 1,data file_id:4,file_start_key:Key { k: \"15\" },file_end_key:Key { k: \"20\" }\n\nlevel: 2,data file_id:0,file_start_key:Key { k: \"11\" },file_end_key:Key { k: \"14\" }\n\n");

        let level_change = version_2.compact_one_level().unwrap().unwrap();
        let version_3 = version_2.apply_change(level_change);
        // println!("{:?}", version_3);
        assert_eq!(format!("{:?}", version_3),"level: 0,data file_id:3,file_start_key:Key { k: \"12\" },file_end_key:Key { k: \"17\" }\n\nlevel: 1,data \nlevel: 2,data file_id:0,file_start_key:Key { k: \"11\" },file_end_key:Key { k: \"14\" }\nfile_id:4,file_start_key:Key { k: \"15\" },file_end_key:Key { k: \"20\" }\n\n");
    }

    #[test]
    pub fn test_get() {
        let version = build_level().unwrap();

        let res = version.get(&Key::new("0")).unwrap();
        assert!(res.is_none());

        let res = version.get(&Key::new("99")).unwrap();
        assert!(res.is_none());

        let res = version.get(&Key::new("16")).unwrap();
        assert_eq!(res, Some(Value::new("a")));

        let res = version.get(&Key::new("18")).unwrap();
        assert_eq!(res, Some(Value::new("b")));

        let res = version.get(&Key::new("19")).unwrap();
        assert_eq!(res, Some(Value::new("19")))
    }

    #[test]
    pub fn test_compact() {}

    #[test]
    pub fn test_level_size_limit() {
        let config = Config::new();
        assert_eq!(Version::level_file_number_limit(0, &config), 4);
        assert_eq!(Version::level_file_number_limit(1, &config), 5);
        assert_eq!(Version::level_file_number_limit(2, &config), 50);
    }
}
