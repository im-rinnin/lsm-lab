use std::{sync, thread};
use std::borrow::{Borrow, BorrowMut};
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::rc::Rc;
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::sync::mpsc::{Receiver, Sender};
use std::time::Duration;

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

pub mod key;
pub mod value;
mod sstable;
mod memtable;
mod level;
mod common;
mod file_storage;
mod meta_log;
mod memtable_log;

mod version;


// (memtable,immutable_memtable,version)
type ThreadSafeData = Arc<RwLock<(Arc<Mutex<Arc<Memtable>>>, Option<Arc<Memtable>>, Arc<Mutex<Arc<Version>>>)>>;


const level_0_limit: usize = 8;
const memtable_len_limit: usize = 4 * 1024 * 1024;
const meta_log_file_name: &str = "meta";

// todo thread safe design multiple thread access
pub struct DBServer {
    path: String,
    data: ThreadSafeData,
    meta_log: MetaLog,
    file_manager: ThreadSafeFileManager,
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


// todo use myerror create
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
    pub fn get(&self, key: &Key) -> Result<Option<Value>, MyError> {
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
        Ok(res)
    }

    pub fn put(&mut self, key: &Key, value: Value) -> Result<()> {
        let write_request = WriteRequest { key: key.clone(), value, finish: self.finish_notify_sender.clone() };
        self.write_request_sender.send(write_request);
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
        todo!()
    }
    pub fn new(path: &Path) -> Result<Self> {
        // create open memtable_log
        // create file_manager
        // call init routine
        todo!()
    }
    pub fn close(self) -> Result<(), MyError> {
        // wait all client exit()
        // stop routine
        todo!()
    }

    fn save_level_change_to_meta_log(meta_log: &mut MetaLog, level_change: &LevelChange) -> Result<()> {
        let data = serde_json::to_string(level_change)?;
        meta_log.add_data(data.as_bytes())
    }


    fn write_routine(data: ThreadSafeData, write_request_channel: Receiver<WriteRequest>, compact_condition_pair: Arc<(Mutex<bool>, Condvar)>) -> Result<()> {
        const write_size_limit: usize = 100;
        let mut write_size_count = 0;
        loop {
            let lock_result = data.write().unwrap();
            let (memtable_ref, b, c) = lock_result.deref();
            let mut memtable = (*memtable_ref).lock().unwrap().clone();
            drop(lock_result);

            // write data to memtable
            loop {
                let request = write_request_channel.recv()?;
                let request_size = request.key.len() + request.value.len();
                write_size_count += request_size;

                memtable.insert(&request.key, &request.value);
                if write_size_count > write_size_limit {
                    break;
                }
            }

            // need compact
            // wait compact finish
            let (lock, cvar) = &*compact_condition_pair;
            let mut compact_is_finish = lock.lock().unwrap();
            while !*compact_is_finish {
                compact_is_finish = cvar.wait(compact_is_finish).unwrap();
            }

            // set immutable memtable
            let mut lock_result = data.write().unwrap();
            let (memtable_ref, immutable_memtable, c) = lock_result.deref_mut();
            assert!(immutable_memtable.is_some());
            let mut memtable = memtable_ref.lock().unwrap();
            let t = memtable.clone();
            *immutable_memtable = Some(t);
            *memtable = Arc::new(Memtable::new());

            write_size_count = 0;
        }
    }

    fn compact_routine(mut data: ThreadSafeData, mut file_manager: FileStorageManager, compact_condition_pair: Arc<(Mutex<bool>, Condvar)>, mut meta_log: MetaLog, start_compact: Receiver<()>) {
        loop {
            let res = start_compact.recv();
            if let Ok(()) = res {
                Self::compact(&mut data, &mut file_manager, compact_condition_pair.clone(), &mut meta_log);
            } else {
                //     todo log exit
                return;
            }
        }
    }

    fn compact(data: &mut ThreadSafeData, file_manager: &mut FileStorageManager, compact_condition_pair: Arc<(Mutex<bool>, Condvar)>, mut meta_log: &mut MetaLog) {
        let (_, immutable_memtable_option, version) = get_current_data(&data);
        //     append sstable to level 0
        let imm_memtable = immutable_memtable_option.expect("must exits");
        let (new_version, level_change) = version.add_memtable_to_level_0(imm_memtable.as_ref());
        let new_version_arc = Arc::new(new_version);
        // write level change to meta log
        Self::save_level_change_to_meta_log(&mut meta_log, &level_change);
        //     lock data
        {
            let mut lock_result = data.write().unwrap();
            let (memtable_ref, immutable_memtable, version) = lock_result.deref_mut();
            //     set immutable memtable to None
            *immutable_memtable = None;
            // set version
            let mut v = version.lock().unwrap();
            *v = new_version_arc.clone();
            //     unlock data
        }
        //     notify write thread
        let (lock, cvar) = &*compact_condition_pair;
        let mut compact_is_finish = lock.lock().unwrap();
        *compact_is_finish = true;
        cvar.notify_all();
        //     check level from 0 to n, do one level compact
        let compact_res = new_version_arc.compact_one_level();
        if let Some((version_after_compact_level, LevelChange)) = compact_res {
            Self::save_level_change_to_meta_log(&mut meta_log, &level_change);
            {
                let mut lock_result = data.write().unwrap();
                let (_, _, version) = lock_result.deref_mut();
                let mut v = version.lock().unwrap();
                *v = Arc::new(version_after_compact_level);
                //     unlock data
            }
        }
    }


    // start routine in new() and from()
    fn init_routines(&self) {
        // start compact routine
    }

    fn stop_routines(&self) {}
}


#[cfg(test)]
mod test {
    use std::thread;

    use tempfile::TempDir;

    use crate::db::DBServer;
    use crate::db::key::Key;
    use crate::db::value::Value;

    #[test]
    fn test_db_build_and_reopen() {
        //     build db from path
        //     close db
        //     reopen db
    }

    #[test]
    fn test_db_simple_get_set() {
        //     new db
        //     set a a
        //     get a
    }

    // todo #[test]
    fn test_db_multiple_thread_get_set() {
        //     new db
        let dir = TempDir::new().unwrap();
        let dir_path = dir.path();
        let mut db_server = DBServer::new(dir_path).unwrap();
        //     create 3 thread
        let mut handles = Vec::new();
        for i in 0..3 {
            let mut db_client = db_server.new_client().unwrap();
            let handle = thread::spawn(move || {
                //     for each thread do set from 1 to 1000, and check by get key
                for i in 0..1000 {
                    db_client.put(&Key::from(i.to_string().as_bytes()), Value::new(&i.to_string()));
                }
                for i in 0..1000 {
                    let value = db_client.get(&Key::from(i.to_string().as_bytes())).unwrap().unwrap();
                    assert_eq!(value, Value::new(&i.to_string()))
                }
            });
            handles.push(handle);
        }
        while handles.len() > 0 {
            let handle = handles.pop().unwrap();
            handle.join();
        }
        db_server.close();
    }
}
