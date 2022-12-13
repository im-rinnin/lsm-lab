use std::cell::RefCell;
use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;

pub type FileId = u32;

#[derive(Clone)]
pub struct FileStorageManager {
    home_path: PathBuf,
    next_file_id: Arc<RefCell<FileId>>,
}

const START_ID: FileId = 0;

impl FileStorageManager {
    pub fn from(home_path: &Path, next_file_id: FileId) -> Self {
        let path = PathBuf::from(home_path);
        FileStorageManager { home_path: path, next_file_id: Arc::new(RefCell::new(next_file_id)) }
    }
    pub fn new(home_path: &Path) -> Self {
        Self::from(home_path, START_ID)
    }
    // return file for read and write
    pub fn open_file(&mut self, file_id: FileId) -> Result<File> {
        assert!(file_id < *self.next_file_id.borrow_mut());
        let mut path = self.home_path.clone();
        path.push(file_id.to_string());
        let file = File::open(path.as_path())?;
        Ok(file)
    }
    // return file for read and write
    pub fn new_file(&mut self) -> Result<(File, FileId)> {
        let mut path = self.home_path.clone();
        let id = *self.next_file_id.borrow_mut();
        *self.next_file_id.borrow_mut() += 1;
        path.push(id.to_string());
        File::create(&path)?;
        let file = File::options()
            .read(true)
            .write(true)
            .open(path)?;
        Ok((file, id))
    }
    pub fn delete(&mut self, id: FileId) -> Result<()> {
        let mut path = self.home_path.clone();
        path.push(id.to_string());
        fs::remove_file(path)?;
        Ok(())
    }

    // causes file count decrease
    pub fn release_file(&mut self, id: FileId) -> Result<()> {
        todo!()
    }
    // delete all unused file
    pub fn remove_unused_file(&mut self)->Result<()>{
        todo!()
    }
}


#[cfg(test)]
mod test {
    use std::io::{Seek, SeekFrom};

    use byteorder::{ReadBytesExt, WriteBytesExt};
    use tempfile::tempdir;

    use crate::db::file_storage::{FileStorageManager, START_ID};

    #[test]
    fn test_create_file() {
        let dir = tempdir().unwrap();
        let mut manager = FileStorageManager::new(dir.path());
        let id = *manager.next_file_id.borrow();
        assert_eq!(id, START_ID);
        let mut file = manager.new_file().unwrap().0;
        let number = 11;
        file.write_u8(number).unwrap();
        file.sync_all().unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        let res = file.read_u8().unwrap();
        assert_eq!(res, number);
        assert_eq!(*manager.next_file_id.borrow(), 1);
        manager.new_file().unwrap();
        assert_eq!(*manager.next_file_id.borrow(), 2);
    }

    #[test]
    fn test_delete_file() {
        let dir = tempdir().unwrap();
        let mut manager = FileStorageManager::new(dir.path());
        let (_, id) = manager.new_file().unwrap();
        let mut path = dir.into_path();
        path.push(id.to_string());
        assert!(path.exists());
        manager.delete(id).unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn test_clone_file_storage() {
        let path = tempdir().unwrap();
        let mut file_storage = FileStorageManager::new(path.path());
        let file_1 = file_storage.new_file().unwrap();
        let mut file_storage_clone = file_storage.clone();
        let file_2 = file_storage_clone.new_file().unwrap();
        let file_3 = file_storage.new_file().unwrap();
        assert_eq!(file_1.1, 0);
        assert_eq!(file_2.1, 1);
        assert_eq!(file_3.1, 2);
    }
}