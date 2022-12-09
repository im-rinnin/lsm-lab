use std::fs;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use anyhow::Result;

use crate::db::sstable::{SSTableReader, SStableWriter};

pub type FileId = u32;

pub struct FileStorageManager {
    home_path: PathBuf,
    next_file_id: FileId,
}

const START_ID: FileId = 0;

impl FileStorageManager {
    pub fn from(home_path: &Path, next_file_id: FileId) -> Self {
        let path = PathBuf::from(home_path);
        FileStorageManager { home_path: path, next_file_id }
    }
    pub fn new(home_path: &Path) -> Self {
        Self::from(home_path, START_ID)
    }
    // return file for read and write
    pub fn new_file(&mut self) -> Result<(File, FileId)> {
        let mut path = self.home_path.clone();
        path.push(self.next_file_id.to_string());
        let id = self.next_file_id;
        self.next_file_id += 1;
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
}


#[cfg(test)]
mod test {
    use std::io::{Seek, SeekFrom};

    use byteorder::{ReadBytesExt, WriteBytesExt};
    use tempfile::tempdir;

    use crate::db::file_storage::{FileStorageManager, START_ID};

    #[test]
    fn test_create_file() {
        let mut dir = tempdir().unwrap();
        let mut manager = FileStorageManager::new(dir.path());
        assert_eq!(manager.next_file_id, START_ID);
        let mut file = manager.new_file().unwrap().0;
        let number = 11;
        file.write_u8(number).unwrap();
        file.sync_all().unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        let res = file.read_u8().unwrap();
        assert_eq!(res, number);
        assert_eq!(manager.next_file_id, 1);
        manager.new_file().unwrap();
        assert_eq!(manager.next_file_id, 2);
    }

    #[test]
    fn test_delete_file() {
        let mut dir = tempdir().unwrap();
        let mut manager = FileStorageManager::new(dir.path());
        let (_, id) = manager.new_file().unwrap();
        let mut path = dir.into_path();
        path.push(id.to_string());
        assert!(path.exists());
        manager.delete(id).unwrap();
        assert!(!path.exists());
    }
}