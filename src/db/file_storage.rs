use std::collections::{HashMap, HashSet};
use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Error, Result};
use log::info;

pub type FileId = u32;
pub type ThreadSafeFileManager = Arc<Mutex<FileStorageManager>>;

// manager file name allocate
pub struct FileStorageManager {
    home_path: PathBuf,
    next_file_id: FileId,
    file_id_usage_count: HashMap<FileId, usize>,
}

const START_ID: FileId = 0;

impl FileStorageManager {
    pub fn to_thread_safe(self) -> ThreadSafeFileManager {
        Arc::new(Mutex::new(self))
    }
    pub fn from(home_path: PathBuf) -> Result<Self> {
        let file_ids = Self::get_all_file_ids(&home_path)?;
        // todo remove file not in active_files
        let max_file_id = file_ids.iter().max().unwrap_or(&START_ID);

        let mut file_count = HashMap::new();
        for id in &file_ids {
            file_count.insert(*id, 1);
        }

        Ok(FileStorageManager {
            home_path,
            next_file_id: max_file_id + 1,
            file_id_usage_count: file_count,
        })
    }
    pub fn new(home_path: &Path) -> Self {
        FileStorageManager {
            home_path: PathBuf::from(home_path),
            next_file_id: START_ID,
            file_id_usage_count: HashMap::new(),
        }
    }
    pub fn new_thread_safe_manager(home_path: PathBuf) -> ThreadSafeFileManager {
        Arc::new(Mutex::new(FileStorageManager {
            home_path,
            next_file_id: START_ID,
            file_id_usage_count: HashMap::new(),
        }))
    }

    pub fn new_file(&mut self) -> Result<(File, FileId, PathBuf)> {
        let file_id = self.next_file_id;
        let path = FileStorageManager::file_path(self.home_path.as_path(), &file_id);
        self.file_id_usage_count.insert(self.next_file_id, 1);
        self.next_file_id += 1;
        let res = File::options()
            .write(true)
            .read(true)
            .create(true)
            .open(path.clone())?;
        Ok((res, file_id, path))
    }
    // todo need test
    pub fn prune_files(&mut self, all_active_file: HashSet<FileId>) {
        // remove all file not in active_file
        self.file_id_usage_count
            .retain(|id, _| all_active_file.contains(id));
    }

    pub fn file_path(home_path: &Path, file_id: &FileId) -> PathBuf {
        let path = home_path.clone().join(file_id.to_string());
        path
    }

    pub fn open_file(home_path: &Path, file_id: &FileId) -> Result<File> {
        let path = Self::file_path(home_path, file_id);
        let res = File::open(path)?;
        Ok(res)
    }
    // todo need test
    pub fn release_file_ids(&mut self, file_ids: &HashSet<FileId>) -> Result<()> {
        for id in file_ids.iter() {
            assert!(self.file_id_usage_count.contains_key(id));
            let count = self.file_id_usage_count.get_mut(id).unwrap();
            if *count == 1 {
                self.file_id_usage_count.remove(id);
                let path = Self::file_path(&self.home_path, id);
                fs::remove_file(&path)?;
                info!("delete file with id {}", id);
            } else {
                *count -= 1;
            }
        }
        Ok(())
    }

    // decrease file count by one, remove file  if is count is 0
    // todo need test
    pub fn get_all_file_ids(home_path: &PathBuf) -> Result<Vec<u32>, Error> {
        let paths = fs::read_dir(home_path.clone()).unwrap();
        let mut file_names: Vec<FileId> = Vec::new();
        for path in paths {
            match path {
                Ok(p) => {
                    let p: PathBuf = p.path();
                    let file_name = p.file_name().ok_or(Error::msg("file_name not found"))?;
                    let file_id = file_name
                        .to_str()
                        .ok_or(Error::msg("file name to str fail"))?
                        .parse::<u32>();
                    if let Ok(id) = file_id {
                        file_names.push(id);
                    }
                }
                Err(e) => {
                    return Err(Error::new(e));
                }
            }
        }
        Ok(file_names)
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;
    use std::io::{Seek, SeekFrom};
    use std::path::Path;
    use std::sync::{Arc, Mutex};
    use std::thread;

    use byteorder::{ReadBytesExt, WriteBytesExt};
    use tempfile::tempdir;

    use crate::db::file_storage::FileStorageManager;

    #[test]
    fn test_create_file() {
        let dir = tempdir().unwrap();
        let mut manager = FileStorageManager::new(dir.path());
        let (mut file, file_id, _) = manager.new_file().unwrap();
        let number = 11;
        assert_eq!(file_id, 0);
        file.write_u8(number).unwrap();
        file.sync_all().unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        let res = file.read_u8().unwrap();
        assert_eq!(res, number);
        let (_, file_id, _) = manager.new_file().unwrap();
        assert_eq!(file_id, 1);
    }

    #[test]
    fn test_build_manager_from_exiting_dir() {
        let dir = tempdir().unwrap();
        let path = dir.into_path();
        let mut manager = FileStorageManager::new(&path);
        manager.new_file().unwrap();
        manager.new_file().unwrap();
        manager.new_file().unwrap();

        let manager = FileStorageManager::from(path).unwrap();
        // manager.file_id_usage_count.sort_by(|a, b| a.cmp(&b));
        for i in 0..3 {
            assert!(manager.file_id_usage_count.contains_key(&i));
        }
    }

    #[test]
    fn test_prune_file() {
        let dir = tempdir().unwrap();
        let mut manager = FileStorageManager::new(dir.path());
        let (_, _, path_a) = manager.new_file().unwrap();
        let (_, id_b, path_b) = manager.new_file().unwrap();

        let mut set = HashSet::new();
        set.insert(id_b);
        manager.prune_files(set);

        assert!(manager.file_id_usage_count.contains_key(&1));

        assert!(!Path::new(&path_a).exists());
        assert!(Path::new(&path_b).exists());
    }

    #[test]
    fn test_multiple_thread() {
        let dir = tempdir().unwrap();
        let manager = FileStorageManager::new(dir.path());
        let thread_safe_manager = manager.to_thread_safe();
        let mut handles = Vec::new();
        for _ in 0..10 {
            let manager_clone = thread_safe_manager.clone();
            let handle = thread::spawn(move || {
                let mut manager = manager_clone.lock().unwrap();
                manager.new_file().unwrap();
            });
            handles.push(handle);
        }
        while handles.len() > 0 {
            let handle = handles.pop().unwrap();
            handle.join().unwrap();
        }
        assert_eq!(
            thread_safe_manager
                .lock()
                .unwrap()
                .file_id_usage_count
                .len(),
            10
        );
    }
}
