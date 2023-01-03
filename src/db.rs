use log::{debug, info, trace};
use lru::LruCache;
use std::borrow::{Borrow, BorrowMut};
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::Read;
use std::num::{NonZeroIsize, NonZeroUsize};
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::Duration;
use std::{sync, thread};

use anyhow::Result;
use serde_json::{from_str, to_string};

use key::Key;
use memtable::Memtable;
use value::Value;

use crate::db::file_storage::{FileId, FileStorageManager, ThreadSafeFileManager};
use crate::db::level::{Level, LevelChange, SStableFileMeta};
use crate::db::memtable_log::MemtableLog;
use crate::db::meta_log::MetaLog;
use crate::db::sstable::SSTable;
use crate::db::version::Version;

use self::config::Config;
use self::metrics::DBMetric;

mod common;
mod config;
mod file_storage;
mod key;
mod level;
mod memtable;
mod memtable_log;
mod meta_log;
mod metrics;
mod sstable;
mod value;

mod version;

// (memtable,immutable_memtable,version)
type ThreadSafeData = Arc<
    RwLock<(
        Arc<Mutex<Arc<Memtable>>>,
        Option<Arc<Memtable>>,
        Arc<Mutex<Arc<Version>>>,
    )>,
>;
pub fn new_thread_safe_data(
    config: &Config,
    path: &Path,
    file_manager: ThreadSafeFileManager,
) -> ThreadSafeData {
    let memtable = Memtable::new();
    let sstable_cache = Arc::new(Mutex::new(LruCache::new(
        NonZeroUsize::new(config.sstable_meta_cache).unwrap(),
    )));
    let version = Version::new(path, file_manager, sstable_cache);
    Arc::new(RwLock::new((
        Arc::new(Mutex::new(Arc::new(memtable))),
        None,
        Arc::new(Mutex::new(Arc::new(version))),
    )))
}

// todo thread safe design multiple thread access
pub struct DBServer {
    path: PathBuf,
    data: ThreadSafeData,
    // meta_log: MetaLog,
    write_request_sender: Sender<WriteRequest>,
    file_manager: ThreadSafeFileManager,
    config: Config,
    metrics: Arc<DBMetric>,
}

pub struct DBClient {
    data: ThreadSafeData,
    finish_notify_sender: Sender<()>,
    finish_notify_receiver: Receiver<()>,
    write_request_sender: Sender<WriteRequest>,
}

pub struct WriteRequest {
    key: Key,
    value: Value,
    finish: Sender<()>,
}

// TODO: use myerror create
#[derive(Debug)]
pub struct MyError {}

fn get_current_data(data: &ThreadSafeData) -> (Arc<Memtable>, Option<Arc<Memtable>>, Arc<Version>) {
    let read_res = data.read().unwrap();
    let (a, b, c) = read_res.deref();
    let memtable = a.lock().unwrap().clone();
    let imm_memtable = if let Some(n) = b {
        Some(n.clone())
    } else {
        None
    };
    let version = c.lock().unwrap().clone();
    (memtable, imm_memtable, version)
}

impl DBClient {
    pub fn get(&self, key: &Key) -> Result<Option<Value>> {
        let (memtable, immutable_memtable, version) = get_current_data(&self.data);
        // search in current memtable
        let res = memtable.get(&key);
        if res.is_some() {
            return Ok(res);
        }

        // search in immutable memtable
        if let Some(memtable) = immutable_memtable.as_deref() {
            let res = memtable.get(key);
            if let Some(value) = res {
                return Ok(Some(value));
            }
        }

        // search in current version
        let res = version.get(&key);
        res
    }

    pub fn put(&mut self, key: &Key, value: Value) -> Result<()> {
        let write_request = WriteRequest {
            key: key.clone(),
            value,
            finish: self.finish_notify_sender.clone(),
        };
        self.write_request_sender.send(write_request).unwrap();
        let _ = self.finish_notify_receiver.recv()?;
        Ok(())
    }
}

impl DBServer {
    pub fn open_db(path: String) -> Result<Self> {
        //     open memtable_log
        //     build version from log

        // call init routine
        todo!()
    }

    pub fn new_client(&self) -> Result<DBClient> {
        let (send, recv) = std::sync::mpsc::channel();
        Ok(DBClient {
            data: self.data.clone(),
            finish_notify_sender: send,
            finish_notify_receiver: recv,
            write_request_sender: self.write_request_sender.clone(),
        })
    }
    pub fn new(path: &Path) -> Result<Self> {
        let default_config = Config::new();
        Self::new_with_confing(path, default_config)
    }
    pub fn new_with_confing(path: &Path, default_config: Config) -> Result<Self> {
        // create open memtable_log
        let meta_log_file_path = path.join(&default_config.meta_log_file_name);
        let meta_log_file = File::create(meta_log_file_path)?;
        let meta_log = MetaLog::new(meta_log_file);

        // create file_manager
        let file_storage = Arc::new(Mutex::new(FileStorageManager::new(path)));

        // init data
        let data = new_thread_safe_data(&default_config, path, file_storage.clone());

        let (sender, recv) = std::sync::mpsc::channel();

        let metric = Arc::new(DBMetric::new());

        let db = DBServer {
            path: PathBuf::from(path),
            data: data.clone(),
            // meta_log: meta_log,
            file_manager: file_storage.clone(),
            config: default_config.clone(),
            write_request_sender: sender,
            metrics: metric.clone(),
        };

        // call init routine
        let mutex = Mutex::new(true);
        let convar = Condvar::new();
        let condition_pair = Arc::new((mutex, convar));

        let (start_compact_sender, start_compact_recv) = sync::mpsc::channel();

        let data_clone = data.clone();
        let condition_pair_clone = condition_pair.clone();
        let metric_clone = metric.clone();

        let compact_routine_join_handle = thread::spawn(move || {
            Self::compact_routine(
                data,
                file_storage,
                condition_pair_clone,
                meta_log,
                start_compact_recv,
                metric_clone,
            )
        });

        let metric_clone = metric.clone();
        let write_routine_join = thread::spawn(move || {
            let res = Self::write_routine(
                data_clone,
                recv,
                condition_pair,
                default_config,
                start_compact_sender,
                metric_clone,
            );
            info!("write_routine return res is {:?}", res);
            res
        });

        Ok(db)
    }

    pub fn close(self) -> Result<(), MyError> {
        // wait all client exit()
        // stop routine
        self.stop_routines();
        Ok(())
    }

    pub fn depth(&self) -> usize {
        let (a, b, c) = get_current_data(&self.data);
        c.depth()
    }

    pub fn display_version(&self) {
        let (a, b, c) = get_current_data(&self.data);
        println!("version is \n {:?}", c);
    }

    fn save_level_change_to_meta_log(
        meta_log: &mut MetaLog,
        level_change: &LevelChange,
    ) -> Result<()> {
        let data = serde_json::to_string(level_change)?;
        meta_log.add_data(data.as_bytes())
    }

    fn write_routine(
        data: ThreadSafeData,
        write_request_channel: Receiver<WriteRequest>,
        compact_condition_pair: Arc<(Mutex<bool>, Condvar)>,
        config: Config,
        start_compact_sender: Sender<()>,
        metric: Arc<DBMetric>,
    ) -> Result<()> {
        let mut write_size_count = 0;
        loop {
            let lock_result = data.write().unwrap();
            let (memtable_ref, b, c) = lock_result.deref();
            let memtable = (*memtable_ref).lock().unwrap().clone();
            drop(lock_result);

            info!("ready for new write request");
            // write data to memtable
            loop {
                let request_result = write_request_channel.recv();
                if request_result.is_err() {
                    info!("request_channel is closed, close compact channel, stop writer routine");
                    return Ok(());
                }
                let request = request_result.unwrap();
                trace!("received write request");
                let request_size = request.key.len() + request.value.len();
                write_size_count += request_size;

                memtable.insert(&request.key, &request.value);
                // TODO: log error;
                let current_level_0_len = metric.get_level_n_file_number(0);
                if current_level_0_len > 4 {
                    thread::sleep(Duration::from_millis(20));
                }
                let send_res = request.finish.send(());

                if write_size_count > config.memtable_size_limit {
                    info!("memtable write size limit  try to start compact");
                    break;
                }
            }

            // need compact
            // wait compact finish
            let (lock, cvar) = &*compact_condition_pair;
            let mut compact_is_finish = lock.lock().unwrap();
            while !*compact_is_finish {
                info!("compact is running, wait for finish");
                compact_is_finish = cvar.wait(compact_is_finish).unwrap();
            }
            info!("receive compact chan, compact is finished");

            // set immutable memtable
            let mut lock_result = data.write().unwrap();
            let (memtable_ref, immutable_memtable, c) = lock_result.deref_mut();
            assert!(immutable_memtable.is_none());
            let mut memtable = memtable_ref.lock().unwrap();
            let t = memtable.clone();
            *immutable_memtable = Some(t);
            *memtable = Arc::new(Memtable::new());

            write_size_count = 0;

            // TODO: log resj
            let send_res = start_compact_sender.send(());
            info!("send signal to compact thread,send res is {:?}", send_res);
            *compact_is_finish = false;
        }
    }

    fn compact_routine(
        data: ThreadSafeData,
        file_manager: ThreadSafeFileManager,
        compact_condition_pair: Arc<(Mutex<bool>, Condvar)>,
        mut meta_log: MetaLog,
        start_compact: Receiver<()>,
        metric: Arc<DBMetric>,
    ) -> Result<()> {
        let mut start_immediate = false;
        loop {
            if !start_immediate {
                let res = start_compact.recv();
                if res.is_err() {
                    info!("compact channel is closed, stop compaction routine");
                    return Ok(());
                }
            }
            start_immediate = false;
            info!("compact thread recv signal");

            // compact memtable
            let (_, immutable_memtable_option, version) = get_current_data(&data);
            //     append sstable to level 0
            let imm_memtable = immutable_memtable_option.expect("must exits");
            let level_change = version.add_memtable_to_level_0(imm_memtable.as_ref())?;
            let new_version = version.apply_change(level_change.clone());
            let mut new_version_arc = Arc::new(new_version);
            // write level change to meta log
            Self::save_level_change_to_meta_log(&mut meta_log, &level_change)?;
            //     lock data
            {
                let mut lock_result = data.write().unwrap();
                let (memtable_ref, immutable_memtable, version) = lock_result.deref_mut();
                //     set immutable memtable to None
                *immutable_memtable = None;
                // set version
                let mut current_version = version.lock().unwrap();
                debug!("set version to {:?}", new_version_arc);
                new_version_arc.record_metrics(metric.as_ref());
                *current_version = new_version_arc.clone();
                //     unlock data
            }
            {
                //     notify write thread
                let (lock, cvar) = &*compact_condition_pair;
                let mut compact_is_finish = lock.lock().unwrap();
                *compact_is_finish = true;
                cvar.notify_all();
            }
            // compact sstable
            loop {
                //     check level from 0 to n, do one level compact
                let compact_res = new_version_arc.compact_one_level()?;
                if compact_res.is_none() {
                    debug!("check level finished, no need to compact");
                    break;
                }
                let level_change = compact_res.unwrap();
                Self::save_level_change_to_meta_log(&mut meta_log, &level_change)?;
                {
                    let mut lock_result = data.write().unwrap();
                    let (_, _, version) = lock_result.deref_mut();
                    let mut current_verison = version.lock().unwrap();
                    let new_version = current_verison.apply_change(level_change);
                    debug!("set version to {:?}", new_version);
                    new_version.record_metrics(&metric);
                    *current_verison = Arc::new(new_version);
                    new_version_arc = current_verison.clone();
                    //     unlock data
                }
                // check if need compact memtable
                let res = start_compact.try_recv();
                match res {
                    Ok(()) => {
                        debug!("need compact memtable immediately");
                        start_immediate = true;
                        break;
                    }
                    Err(TryRecvError::Empty) => {
                        debug!("try to check level and compact");
                        continue;
                    }
                    Err(TryRecvError::Disconnected) => {
                        info!("compact channel is closed, stop compaction routine");
                        return Ok(());
                    }
                }
            }
        }
    }

    fn stop_routines(&self) {
        // self.write_request_sender
    }
}

#[cfg(test)]
mod test {
    use std::thread;
    use std::time::Duration;

    use byteorder::LE;
    use log::{debug, error, info, warn};
    use tempfile::TempDir;

    use crate::db::common::init_test_log;
    use crate::db::config::Config;
    use crate::db::key::Key;
    use crate::db::sstable::SSTable;
    use crate::db::value::Value;
    use crate::db::DBServer;

    use super::common::init_test_log_as_debug;

    // #[test]
    fn test_db_build_and_reopen() {
        //     build db from path
        // let (db_server, client, number) = build_3_level();
        todo!()

        //     reopen db
    }

    fn build_3_level(dir: &TempDir) -> (DBServer, super::DBClient, i32) {
        let mut c = Config::new();
        c.memtable_size_limit = 1000;
        let db = DBServer::new_with_confing(dir.path(), c).unwrap();
        let mut client = db.new_client().unwrap();
        let number = 1000;
        for i in 0..number {
            let key = Key::new(&i.to_string());
            let value = Value::new(&i.to_string());
            client.put(&key, value).unwrap();
        }

        (db, client, number)
    }

    #[test]
    fn test_simple_set_and_get() {
        // init_test_log_as_debug();

        //     new db
        let dir = TempDir::new().unwrap();
        let dir_path = dir.path();

        let mut c = Config::new();
        c.memtable_size_limit = 1000;
        let db_server = DBServer::new_with_confing(dir_path, c).unwrap();
        let mut db_client = db_server.new_client().unwrap();
        let number = 1150;
        for i in 0..number {
            // add 0 to make key enough long to trigger bug
            let mut key = String::from("0");
            key.push_str("_");
            key.push_str(&i.to_string());

            db_client
                .put(&Key::new(&key), Value::new(&i.to_string()))
                .unwrap();
        }

        for i in 0..number {
            let mut key = String::from("0");
            key.push_str("_");
            key.push_str(&i.to_string());
            let value_res = db_client.get(&Key::new(&key));
            assert_eq!(value_res.unwrap().unwrap(), Value::new(&i.to_string()))
        }
        db_server.close().unwrap();
    }

    // use 3 thread set and get in different keys
    #[test]
    fn test_db_multiple_thread_get_set() {
        // init_test_log_as_debug();

        //     new db
        let dir = TempDir::new().unwrap();
        let dir_path = dir.path();

        let mut c = Config::new();
        c.memtable_size_limit = 1000;

        let db_server = DBServer::new_with_confing(dir_path, c).unwrap();
        //     create 3 thread
        let mut handles = Vec::new();
        for thread_id in 0..1 {
            let mut db_client = db_server.new_client().unwrap();
            let number = 1200;
            let handle = thread::spawn(move || {
                //     for each thread do set from 1 to 1000, and check by get key
                for i in 0..number {
                    let mut key = thread_id.to_string();
                    key.push_str("_");
                    key.push_str(&i.to_string());

                    db_client
                        .put(&Key::new(&key), Value::new(&i.to_string()))
                        .unwrap();
                }

                for i in 0..number {
                    let mut key = thread_id.to_string();
                    key.push_str("_");
                    key.push_str(&i.to_string());
                    debug!("key is {:}", key);
                    let value_res = db_client.get(&Key::new(&key));
                    if value_res.is_err() {
                        panic!("get key error {:?}", value_res);
                    }
                    if value_res.unwrap().is_none() {
                        error!("key not found {:?}", key);
                    }
                    // assert_eq!(value_res.unwrap().unwrap(), Value::new(&i.to_string()))
                }
            });
            handles.push(handle);
        }
        while handles.len() > 0 {
            let handle = handles.pop().unwrap();
            handle.join().unwrap();
        }
        db_server.close().unwrap();
    }
}
