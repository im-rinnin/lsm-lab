use ::metrics::increment_counter;
use crossbeam::select;
use log::{debug, error, info, trace};
use lru::LruCache;
use metrics::{absolute_counter, gauge};
use rmp_serde::encode::Error;
use std::borrow::{Borrow, BorrowMut};
use std::cell::RefCell;
use std::collections::{hash_set, HashMap, HashSet, VecDeque};
use std::fs::{self, File};
use std::io::Read;
use std::num::{NonZeroIsize, NonZeroUsize};
use std::ops::{Deref, DerefMut, Sub};
use std::path::{Path, PathBuf};
use std::rc::Rc;
// use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, TryRecvError};
use crossbeam::channel::{
    bounded, unbounded, Receiver, RecvTimeoutError, Select, Sender, TryRecvError,
};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::{spawn, JoinHandle};
use std::time::{Duration, Instant, SystemTime};
use std::{sync, thread};

use anyhow::Result;
use serde_json::{from_str, to_string};

use key::Key;
use memtable::Memtable;
use value::Value;

use crate::db::db_metrics::{
    COMPACT_COUNT, CURRENT_LEVEL_DEPTH, READ_HIT_MEMTABLE_COUNTER, READ_REQUEST_COUNT,
    READ_REQUEST_TIME, WRITE_REQUEST_COUNT, WRITE_WAIT_FOR_COMAPCT,
};
use crate::db::file_storage::{FileId, FileStorageManager, ThreadSafeFileManager};
use crate::db::level::{Level, LevelChange, SStableFileMeta};
use crate::db::memtable_log::MemtableLog;
use crate::db::meta_log::MetaLog;
use crate::db::sstable::SSTable;
use crate::db::version::Version;

use self::config::Config;
use self::db_metrics::{DBMetric, TimeRecorder, WRITE_REQUEST_TIME};
use self::memtable::MemtableIter;
use self::memtable_log::MemtableLogReader;
use self::meta_log::MetaLogIter;
use self::sstable::SStableBlockMeta;

mod common;
pub mod config;
mod db_metrics;
pub mod debug_util;
mod file_storage;
pub mod key;
mod level;
mod memtable;
mod memtable_log;
mod meta_log;
mod sstable;
pub mod value;

mod version;

// (memtable,immutable_memtable,version)
type ThreadSafeData = Arc<
    RwLock<(
        Arc<Mutex<Arc<Memtable>>>,
        Option<Arc<Memtable>>,
        Arc<Mutex<Arc<Version>>>,
    )>,
>;
pub fn new_sstable_cache(config: &Config) -> Arc<Mutex<LruCache<FileId, Arc<SStableBlockMeta>>>> {
    let sstable_cache = Arc::new(Mutex::new(LruCache::new(
        NonZeroUsize::new(config.sstable_meta_cache).unwrap(),
    )));
    sstable_cache
}

pub struct DBServer {
    path: PathBuf,
    data: ThreadSafeData,
    write_request_sender: Sender<WriteRequest>,
    config: Config,
    metrics: Arc<DBMetric>,
    thread_handles: Vec<JoinHandle<Result<()>>>,
}

pub struct DBClient {
    data: ThreadSafeData,
    finish_notify_sender: Sender<()>,
    finish_notify_receiver: Receiver<()>,
    write_request_sender: Sender<WriteRequest>,
}

pub struct WriteRequest {
    key: Key,
    value: Option<Value>,
    finish: Sender<()>,
}

impl WriteRequest {
    pub fn size(&self) -> usize {
        let key_size = self.key.len();
        let value_size = if let Some(v) = &self.value {
            v.len()
        } else {
            0
        };
        key_size + value_size
    }
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
    pub fn get_str(&self, key: &str) -> Result<Option<Value>> {
        self.get(&Key::new(key))
    }

    pub fn get(&self, key: &Key) -> Result<Option<Value>> {
        let recorder = TimeRecorder::new(READ_REQUEST_TIME);
        increment_counter!(READ_REQUEST_COUNT);

        let (memtable, immutable_memtable, version) = get_current_data(&self.data);
        // search in current memtable
        let res = memtable.get(&key);
        if res.is_some() {
            increment_counter!(READ_HIT_MEMTABLE_COUNTER);
            if let Some(v) = res.unwrap() {
                return Ok(Some(v));
            } else {
                return Ok(None);
            }
        }

        // search in immutable memtable
        if let Some(memtable) = immutable_memtable.as_deref() {
            let res = memtable.get(key);
            if res.is_some() {
                increment_counter!(READ_HIT_MEMTABLE_COUNTER);
                if let Some(v) = res.unwrap() {
                    return Ok(Some(v));
                } else {
                    return Ok(None);
                }
            }
        }

        // search in current version
        let res = version.get(&key);
        res
    }

    pub fn delete(&mut self, key: &Key) -> Result<()> {
        self.put_impl(key, None)
    }

    pub fn put(&mut self, key: &Key, value: Value) -> Result<()> {
        self.put_impl(key, Some(value))
    }
    fn put_impl(&mut self, key: &Key, value: Option<Value>) -> Result<()> {
        let time_recorder = TimeRecorder::new(WRITE_REQUEST_TIME);
        let write_request = WriteRequest {
            key: key.clone(),
            value: value,
            finish: self.finish_notify_sender.clone(),
        };
        self.write_request_sender.send(write_request).unwrap();
        let _ = self.finish_notify_receiver.recv()?;
        Ok(())
    }
}

impl DBServer {
    pub fn open_db(path: PathBuf, config: Config) -> Result<Self> {
        let file_storage = FileStorageManager::from(path.clone())?;
        let thread_safe_file_storage = Arc::new(Mutex::new(file_storage));

        let memtable = Self::build_memtable(&path, &config)?;
        let (s, r) = unbounded();
        let veresion = Self::build_version(&path, &config, thread_safe_file_storage.clone(), s)?;

        let file_manager = Arc::new(Mutex::new(FileStorageManager::from(path.clone())?));
        Self::new_impl(path, config, file_manager, memtable, veresion, r)
    }

    fn build_memtable(path: &Path, config: &Config) -> Result<Memtable> {
        let memtable_log_path = path.to_path_buf().join(&config.memtable_log_file_path);
        let memtable_log_file = File::open(memtable_log_path)?;
        let memtable_log_iter = MemtableLogReader::new(memtable_log_file)?;

        let memtable = Memtable::new();
        for (k, v) in memtable_log_iter {
            memtable.insert_option_value(&k, &v)
        }
        Ok(memtable)
    }
    fn build_version(
        path: &Path,
        config: &Config,
        file_storage: ThreadSafeFileManager,
        file_id_sender: Sender<HashSet<FileId>>,
    ) -> Result<Version> {
        // build version from log
        let home_path = PathBuf::from(path);
        let meta_file_path = path.join(&config.meta_log_file_name);
        let meta_file = File::open(meta_file_path).unwrap();
        let iter = MetaLogIter::new(meta_file);

        // build version
        let mut level_changes = Vec::new();
        for data_res in iter {
            match data_res {
                Err(err) => {
                    error!("fail to get meta data {:}", err);
                    return Err(err);
                }
                Ok(data) => {
                    let level_change: LevelChange = serde_json::from_slice(&data)?;
                    level_changes.push(level_change);
                }
            }
        }

        let sstable_cache = new_sstable_cache(&config);

        let mut iter = level_changes.into_iter();

        let f_clone = file_storage.clone();
        let version: Version = Version::from(
            &mut iter,
            home_path.clone(),
            f_clone,
            sstable_cache,
            file_id_sender,
        )?;

        Ok(version)
    }

    pub fn new_client(&self) -> Result<DBClient> {
        let (send, recv) = unbounded();
        Ok(DBClient {
            data: self.data.clone(),
            finish_notify_sender: send,
            finish_notify_receiver: recv,
            write_request_sender: self.write_request_sender.clone(),
        })
    }
    pub fn new(path: PathBuf) -> Result<Self> {
        let config = Config::new();
        Self::new_with_confing(path, config)
    }
    pub fn new_with_confing(home_path: PathBuf, c: Config) -> Result<Self> {
        // create open memtable_log
        let memtable = Memtable::new();
        let cache = new_sstable_cache(&c);
        let file_manager = Arc::new(Mutex::new(FileStorageManager::new(&home_path)));

        let (file_id_dec_sender, file_id_dec_recv) = unbounded();
        let version = Version::new(&home_path, file_manager.clone(), cache, file_id_dec_sender);

        //  delete all unused files

        let all_files = FileStorageManager::get_all_file_ids(&home_path)?;
        let all_active_files = version.get_all_file_ids();
        for id in all_files {
            if !all_active_files.contains(&id) {
                info!("file {:} is unnused, deleting it", id);
                let path = home_path.join(&id.to_string());
                fs::remove_file(path)?;
            }
        }

        Self::new_impl(
            home_path,
            c,
            file_manager,
            memtable,
            version,
            file_id_dec_recv,
        )
    }

    fn new_impl(
        path: PathBuf,
        default_config: Config,
        file_strorage: ThreadSafeFileManager,
        memtable: Memtable,
        version: Version,
        file_id_dec_recv: Receiver<HashSet<FileId>>,
    ) -> Result<Self> {
        let memtable_log_path = path.join(PathBuf::from(&default_config.memtable_log_file_path));
        let memtable_log_file = File::create(memtable_log_path)?;

        let meta_log_file_path = path.join(&default_config.meta_log_file_name);
        let meta_log_file = File::create(meta_log_file_path)?;
        let meta_log = MetaLog::new(meta_log_file);

        let cache = new_sstable_cache(&default_config);

        let all_active_files = version.get_all_file_ids();

        let data = Arc::new(RwLock::new((
            Arc::new(Mutex::new(Arc::new(memtable))),
            None,
            Arc::new(Mutex::new(Arc::new(version))),
        )));

        let (sender, recv) = unbounded();

        let metric = Arc::new(DBMetric::new());

        let mut thread_handles = Vec::new();

        let mutex = Mutex::new(true);
        let convar = Condvar::new();
        let condition_pair = Arc::new((mutex, convar));

        let (start_compact_sender, start_compact_recv) = unbounded();

        let data_clone = data.clone();
        let condition_pair_clone = condition_pair.clone();
        let metric_clone = metric.clone();

        let path_clone = path.clone();
        let (file_id_inc_sender, file_id_inc_recv) = bounded(0);

        let prune_file_handle = spawn(move || {
            let res = Self::prune_file_routine(
                path_clone,
                all_active_files,
                file_id_dec_recv,
                file_id_inc_recv,
            );
            res
        });

        let compact_routine_join_handle = thread::spawn(move || {
            Self::compact_routine(
                data_clone,
                file_strorage,
                condition_pair_clone,
                meta_log,
                start_compact_recv,
                metric_clone,
                file_id_inc_sender,
            )
        });

        let metric_clone = metric.clone();
        let data_clone = data.clone();
        let config_clone = default_config.clone();
        let write_routine_join = thread::spawn(move || {
            let res = Self::write_routine(
                data_clone,
                recv,
                condition_pair,
                config_clone,
                start_compact_sender,
                metric_clone,
                memtable_log_file,
            );
            info!("write_routine return res is {:?}", res);
            res
        });

        thread_handles.push(write_routine_join);
        thread_handles.push(compact_routine_join_handle);
        thread_handles.push(prune_file_handle);

        let db = DBServer {
            path: PathBuf::from(path),
            data: data.clone(),
            config: default_config,
            write_request_sender: sender,
            metrics: metric.clone(),
            thread_handles,
        };

        Ok(db)
    }

    pub fn close(mut self) -> Result<()> {
        info!("close db");
        drop(self.write_request_sender);
        drop(self.data);

        while let Some(h) = self.thread_handles.pop() {
            // TODO: log
            let error = h.join();
        }
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
        memtable_log_file: File,
    ) -> Result<()> {
        let mut memtable_log = MemtableLog::new(memtable_log_file);
        let mut request_buffer: Vec<WriteRequest> = Vec::new();

        let mut channal_is_open = true;
        let mut memtable_size = 0;
        loop {
            if !channal_is_open {
                info!("Channal is closed, write routine return");
                memtable_log.sync_all()?;
                return Ok(());
            }
            channal_is_open = save_to_log(
                &config,
                &write_request_channel,
                &mut memtable_log,
                &mut request_buffer,
            )?;

            let need_compact = write_to_memtable(
                &data,
                &mut request_buffer,
                &metric,
                &config,
                &mut memtable_size,
            );
            if !need_compact {
                continue;
            }
            // need compact
            memtable_size = 0;

            let (lock, cvar) = &*compact_condition_pair;
            // wait compact finish
            let mut compact_is_finish = lock.lock().unwrap();
            {
                let r = TimeRecorder::new(WRITE_WAIT_FOR_COMAPCT);
                while !*compact_is_finish {
                    info!("compact is running, wait for finish");
                    compact_is_finish = cvar.wait(compact_is_finish).unwrap();
                }
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

            // TODO: log res
            let send_res = start_compact_sender.send(());
            info!("send signal to compact thread,send res is {:?}", send_res);
            *compact_is_finish = false;
        }
    }

    fn prune_file_routine(
        home_path: PathBuf,
        file_ids: HashSet<FileId>,
        file_ref_decrease_recv: Receiver<HashSet<FileId>>,
        file_ref_increase_recv: Receiver<HashSet<FileId>>,
    ) -> Result<()> {
        let mut file_id_count = HashMap::new();
        for id in file_ids.iter() {
            file_id_count.insert(*id, 1);
        }
        let mut select = Select::new();
        let mut index_set = HashSet::new();
        let inc_index = select.recv(&file_ref_increase_recv);
        index_set.insert(inc_index);
        let dec_index = select.recv(&file_ref_decrease_recv);
        index_set.insert(dec_index);

        loop {
            if index_set.is_empty() {
                break;
            }
            let select_res = select.select();
            if select_res.index() == dec_index {
                let file_ids_res = select_res.recv(&file_ref_decrease_recv);
                match file_ids_res {
                    Err(ref err) => {
                        info!("prune file routine recv error: {:?} ", err);
                        select.remove(dec_index);
                        index_set.remove(&dec_index);
                        continue;
                    }
                    Ok(ids) => {
                        for id in ids.iter() {
                            let count = file_id_count.get_mut(id).unwrap();
                            if *count == 1 {
                                file_id_count.remove(id);
                                let path = FileStorageManager::file_path(&home_path, id);
                                fs::remove_file(&path)?;
                                info!("delete file with id {}", id);
                            } else {
                                *count -= 1;
                            }
                        }
                    }
                }
            } else if select_res.index() == inc_index {
                let file_ids_res = select_res.recv(&file_ref_increase_recv);
                match file_ids_res {
                    Err(ref err) => {
                        info!("prune file routine recv error: {:?} ", err);
                        select.remove(inc_index);
                        index_set.remove(&inc_index);
                        continue;
                    }
                    Ok(ids) => {
                        for id in ids {
                            if let Some(i) = file_id_count.get_mut(&id) {
                                *i += 1;
                            } else {
                                file_id_count.insert(id, 1);
                            }
                        }
                    }
                }
            }
        }
        info!("prune file routine return");
        Ok(())
    }

    fn compact_routine(
        data: ThreadSafeData,
        file_manager: ThreadSafeFileManager,
        compact_condition_pair: Arc<(Mutex<bool>, Condvar)>,
        mut meta_log: MetaLog,
        start_compact: Receiver<()>,
        metric: Arc<DBMetric>,
        file_id_inc_sender: Sender<HashSet<FileId>>,
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
            let new_version_ids = new_version.get_all_file_ids();
            file_id_inc_sender.send(new_version_ids).unwrap();

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
                gauge!(CURRENT_LEVEL_DEPTH, new_version_arc.depth() as f64);
                increment_counter!(COMPACT_COUNT);

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
                    let new_version_ids = new_version.get_all_file_ids();
                    file_id_inc_sender.send(new_version_ids).unwrap();

                    debug!("set version to {:?}", new_version);
                    gauge!(CURRENT_LEVEL_DEPTH, new_version_arc.depth() as f64);
                    increment_counter!(COMPACT_COUNT);
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
}

// return true if need compact memtable
fn write_to_memtable(
    data: &Arc<
        RwLock<(
            Arc<Mutex<Arc<Memtable>>>,
            Option<Arc<Memtable>>,
            Arc<Mutex<Arc<Version>>>,
        )>,
    >,
    request_buffer: &mut Vec<WriteRequest>,
    metric: &Arc<DBMetric>,
    config: &Config,
    memtable_size: &mut usize,
) -> bool {
    // get current memtable
    let lock_result = data.write().unwrap();
    let (memtable_ref, b, c) = lock_result.deref();
    let memtable = (*memtable_ref).lock().unwrap().clone();
    drop(lock_result);
    // write data to memtable
    while let Some(request) = request_buffer.pop() {
        memtable.insert_option_value(&request.key, &request.value);
        *memtable_size += request.size();

        // TODO: log error;
        let current_level_0_len = metric.get_level_n_file_number(0);
        if current_level_0_len > 4 {
            thread::sleep(Duration::from_millis(2));
        }
        let send_res = request.finish.send(());
        increment_counter!(WRITE_REQUEST_COUNT);
    }
    debug!("current memtable size {:}", *memtable_size);
    // check size
    if *memtable_size > config.memtable_size_limit {
        info!("memtable write size limit try to start compact");
        return true;
    }
    return false;
}

// return false if write channel is closed
fn save_to_log(
    config: &Config,
    write_request_channel: &Receiver<WriteRequest>,
    memtable_log: &mut MemtableLog,
    request_buffer: &mut Vec<WriteRequest>,
) -> Result<bool, anyhow::Error> {
    let mut write_size_count = 0;
    let start_time = Instant::now();
    let mut channel_is_open = true;
    loop {
        let now = Instant::now();
        let pass_time = now.duration_since(start_time);
        let rest_time = config
            .request_write_buffer_wait_time
            .saturating_sub(pass_time);

        if rest_time.is_zero() {
            trace!("use all time, save log return");
            break;
        }

        let request_result = write_request_channel.recv_timeout(
            config
                .request_write_buffer_wait_time
                .saturating_sub(rest_time),
        );
        match request_result {
            Err(RecvTimeoutError::Timeout) => {
                debug!("write request channel timed out");
                break;
            }
            Err(RecvTimeoutError::Disconnected) => {
                info!("request_channel is closed, close compact channel, stop writer routine");
                channel_is_open = false;
                break;
            }
            Ok(request) => {
                trace!("received write request");
                let request_size = request.size();
                write_size_count += request_size;
                memtable_log.add(&request.key, &request.value)?;
                request_buffer.push(request);
                if write_size_count > config.request_write_batch_size {
                    memtable_log.flush_buf()?;
                    info!("reach write buffer size limit, save log return");
                    break;
                }
            }
        }
    }

    Ok(channel_is_open)
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;
    use std::fs::File;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use std::{fs, thread};

    use byteorder::LE;
    use crossbeam::channel::unbounded;
    use log::{debug, error, info, warn};
    use tempfile::{tempdir, TempDir};

    use crate::db::config::Config;
    use crate::db::key::Key;
    use crate::db::sstable::SSTable;
    use crate::db::value::Value;
    use crate::db::{get_current_data, DBServer};

    use super::debug_util::{dump_recv, init_test_log_as_debug_and_metric};
    use super::file_storage::FileStorageManager;
    use super::DBClient;

    fn build_config_for_test() -> Config {
        let mut c = Config::new();
        // use small memetable to make more compact and depth level easier
        c.memtable_size_limit = 10;
        c
    }

    // #[test]
    fn test_reopen_db() {
        let dir = TempDir::new().unwrap();
        let number = 1000;
        let (server, _, config) = build_db(&dir, number);
        server.close().unwrap();

        let server = DBServer::open_db(dir.into_path(), config).unwrap();
        let client = server.new_client().unwrap();

        for i in 0..number {
            let res = client.get_str(&i.to_string());
            assert_eq!(res.unwrap().unwrap(), Value::new(&i.to_string()));
        }
    }

    #[test]
    fn test_build_memtable_and_version_in_db_reopen() {
        let dir = TempDir::new().unwrap();

        let number = 1000;
        let (server, _, config) = build_db(&dir, number);
        server.close().unwrap();
        let file_storage = FileStorageManager::from(dir.path().to_path_buf()).unwrap();
        let thread_safe_storage = Arc::new(Mutex::new(file_storage));
        let memtable = DBServer::build_memtable(dir.path(), &config).unwrap();

        let (s, r) = unbounded();
        dump_recv(r);
        let version = DBServer::build_version(dir.path(), &config, thread_safe_storage, s).unwrap();

        for i in 0..number {
            let res = memtable.get_str(&i.to_string());
            if let Some(v) = res {
                assert_eq!(v.unwrap(), Value::new(&i.to_string()));
            } else {
                let res = version.get_str(&i.to_string()).unwrap();
                assert!(res.is_some());
                assert_eq!(res.unwrap(), Value::new(&i.to_string()));
            }
        }
    }
    fn build_db(dir: &TempDir, number: usize) -> (DBServer, super::DBClient, Config) {
        let mut c = Config::new();
        c.memtable_size_limit = 1000;
        let db = DBServer::new_with_confing(dir.path().to_path_buf(), c.clone()).unwrap();
        let mut client = db.new_client().unwrap();
        for i in 0..number {
            let key = Key::new(&i.to_string());
            let value = Value::new(&i.to_string());
            client.put(&key, value).unwrap();
        }

        (db, client, c)
    }

    #[test]
    fn test_simple_set_and_get() {
        //     new db
        let dir = TempDir::new().unwrap();
        let dir_path = dir.path();

        let c = build_config_for_test();
        let db_server = DBServer::new_with_confing(dir_path.to_path_buf(), c).unwrap();
        let mut db_client = db_server.new_client().unwrap();
        let number = 200;
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
        drop(db_client);
        db_server.close().unwrap();
    }

    // use 3 thread set and get in different keys
    #[test]
    fn test_db_multiple_thread_get_set_delete() {
        let dir = TempDir::new().unwrap();
        let dir_path = dir.path();
        let c = build_config_for_test();
        let db_server = DBServer::new_with_confing(dir_path.to_path_buf(), c).unwrap();
        let number = 200;

        //     create 3 thread
        let mut handles = Vec::new();
        for thread_id in 0..3 {
            let mut db_client = db_server.new_client().unwrap();
            let handle = thread::spawn(move || {
                //     for each thread do set from 1 to 1000, delete even key in [0,to number/2) and check
                for i in 0..number {
                    let mut key = thread_id.to_string();
                    key.push_str("_");
                    // delete even while insert
                    let mut delete_key = key.clone();
                    delete_key.push_str(&(i / 2).to_string());

                    key.push_str(&i.to_string());

                    db_client
                        .put(&Key::new(&key), Value::new(&i.to_string()))
                        .unwrap();

                    if (i / 2) % 2 == 0 {
                        db_client.delete(&Key::new(&delete_key)).unwrap();
                    }
                }

                for i in 0..number {
                    let mut key = thread_id.to_string();
                    key.push_str("_");
                    key.push_str(&i.to_string());
                    let value_res = db_client.get(&Key::new(&key));
                    if i % 2 == 0 && i < number / 2 {
                        assert!(
                            value_res.unwrap().is_none(),
                            "key {:} should be deleted",
                            key
                        );
                    } else {
                        assert_eq!(
                            value_res.unwrap().unwrap(),
                            Value::new(&i.to_string()),
                            "key {:} not match",
                            key
                        )
                    }
                }
            });
            handles.push(handle);
        }
        while handles.len() > 0 {
            let handle = handles.pop().unwrap();
            handle.join().unwrap();
        }
        assert!(db_server.depth() >= 2);
        db_server.close().unwrap();
    }
    // build db to 2 level ,delete all kv, check level 1, it should contains no deleted kv
    #[test]
    fn test_deleted_entry_prune() {
        let dir = TempDir::new().unwrap();
        let dir_path = dir.path();
        let c = build_config_for_test();
        let db_server = DBServer::new_with_confing(dir_path.to_path_buf(), c).unwrap();
        let number = 400;
        let mut client = db_server.new_client().unwrap();

        client
            .put(&Key::new(&0.to_string()), Value::new(&0.to_string()))
            .unwrap();

        client.delete(&Key::new(&0.to_string())).unwrap();

        for i in 1..number {
            client
                .put(&Key::new(&i.to_string()), Value::new(&i.to_string()))
                .unwrap();
        }
        assert_eq!(db_server.depth(), 2);

        let (a, b, c) = get_current_data(&db_server.data);
        let level = c.get_level_for_test(1);
        let kvs = level.get_kvs_for_test();
        for (k, v) in kvs {
            assert!(v.is_some(), "key {:?} shuold be prune", k);
        }
    }

    // check sstable is delted after its reference count is zero
    #[test]
    fn test_sstable_file_prune() {
        let dir = tempdir().unwrap();
        let s = build_db(&dir, 2000);
        let paths = fs::read_dir(dir.path()).unwrap();
        let mut ids = HashSet::new();
        for path in paths {
            let entry = path.unwrap().file_name().to_str().unwrap().to_string();
            ids.insert(entry);
        }
        assert!(!ids.contains("0"));
    }
    #[test]
    fn test_detele_unused_file_when_db_start() {
        let dir = tempdir().unwrap();
        let file_path_1 = dir.path().to_path_buf().join("0");
        File::create(&file_path_1).unwrap();
        let file_path_2 = dir.path().to_path_buf().join("1");
        File::create(&file_path_2).unwrap();

        let s = DBServer::new(dir.path().to_path_buf()).unwrap();

        assert!(fs::metadata(file_path_1).is_err());
        assert!(fs::metadata(file_path_2).is_err());
    }
}
