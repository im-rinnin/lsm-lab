use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use anyhow::Result;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};


pub struct MetaLog {
    file: File,
}

pub struct MetaLogIter {
    file: File,
    remain_data_len: usize,
}

impl Iterator for MetaLogIter {
    type Item = Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remain_data_len == 0 {
            return None;
        }
        let res = {
            let len = self.file.read_u64::<LittleEndian>().expect("should has data");
            let mut res = vec![0; len as usize];
            self.file.read(&mut res).expect("should have data");
            self.remain_data_len -= (len + 8) as usize;
            Ok(res)
        };
        Some(res)
    }
}


impl MetaLog {
    pub fn new( file: File) -> Self {
        MetaLog { file }
    }

    pub fn add_data(&mut self, data: &Vec<u8>) -> Result<()> {
        let len = data.len();
        self.file.write_u64::<LittleEndian>(len as u64)?;
        self.file.write(data.as_slice())?;
        self.file.sync_all()?;
        Ok(())
    }

    // for db start
    pub fn to_iter(file: File) -> Result<MetaLogIter> {
        let len = file.metadata()?.len();
        Ok(MetaLogIter { file, remain_data_len: len as usize})
    }
}

#[cfg(test)]
mod test {
    use std::env::temp_dir;

    use crate::db::file_storage::FileStorageManager;
    use crate::db::meta_log::MetaLog;

    #[test]
    fn test_add_and_check_iter() {
        let path = temp_dir();
        let mut file_manager = FileStorageManager::new(path.clone());
        let (mut file, id, _) = file_manager.new_file().unwrap();
        let mut meta_log = MetaLog::new(file);
        let data_a: Vec<u8> = vec![1,2,4];
        let data_b: Vec<u8> = vec![2,5,2];
        meta_log.add_data(&data_a).unwrap();
        meta_log.add_data(&data_b).unwrap();

        let mut iter = MetaLog::to_iter(FileStorageManager::open_file(&path, &id).unwrap()).unwrap();
        let data = iter.next().unwrap().unwrap();
        assert_eq!(data, data_a);
        let data = iter.next().unwrap().unwrap();
        assert_eq!(data, data_b);
    }
}
