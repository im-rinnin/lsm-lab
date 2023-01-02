use std::cell::RefCell;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::SeekFrom::Start;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;

use anyhow::Result;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use log::info;
use serde::{Deserialize, Serialize};

use crate::db::common::{KVIterItem, ValueSliceTag};
use crate::db::file_storage::FileStorageManager;
use crate::db::key::{Key, KeySlice, KEY_SIZE_LIMIT};
use crate::db::level::SStableFileMeta;
use crate::db::sstable::block::{Block, BlockBuilder, BlockIter, BlockMeta, BLOCK_SIZE};
use crate::db::value::Value;

mod block;
pub mod file_base_sstable;

const BLOCK_POOL_MEMORY_SIZE: usize = 2 * KEY_SIZE_LIMIT + BLOCK_SIZE;

/// format https://github.com/google/leveldb/blob/main/doc/table_format.md
/// block 1
/// block 2
///  ...
/// block n
/// block meta
/// block meta number (u64)
/// block meta offset (u64)

// immutable, own by level
pub struct SSTable {
    sstable_metas: Arc<SStableBlockMeta>,
    file: RefCell<File>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SStableBlockMeta {
    block_metas: Vec<BlockMeta>,
}

pub struct SStableIter<'a> {
    block_iter: BlockIter,
    sstable: &'a SSTable,
    next_block_number: usize,
}

impl<'a> SStableIter<'a> {
    pub fn new(sstable: &'a SSTable) -> Result<Self> {
        assert!(sstable.sstable_metas.block_metas.len() > 0);
        let block = sstable.read_block(0)?;
        let block_iter = block.into_iter();
        Ok(SStableIter {
            block_iter,
            sstable,
            next_block_number: 1,
        })
    }
}

impl<'a> Iterator for SStableIter<'a> {
    type Item = KVIterItem;
    fn next(&mut self) -> Option<Self::Item> {
        let mut res = self.block_iter.next();
        if let None = res {
            if self.next_block_number == self.sstable.sstable_metas.block_metas.len() {
                return None;
            }
            let block = self.sstable.read_block(self.next_block_number).unwrap();
            self.next_block_number += 1;
            self.block_iter = block.into_iter();
            res = self.block_iter.next();
            assert!(res.is_some());
        }
        res
    }
}

impl SSTable {
    pub const SSTABLE_SIZE_LIMIT: usize = 1024 * 1024 * 2;
    pub fn get_meta_from_file(file: &mut File) -> Result<SStableBlockMeta> {
        file.seek(SeekFrom::End(-8))?;
        let meta_offset = file.read_u64::<LittleEndian>()?;
        file.seek(SeekFrom::End(-16))?;
        let meta_number = file.read_u64::<LittleEndian>()?;
        file.seek(SeekFrom::Start(meta_offset))?;
        let mut metas = Vec::new();
        for _ in 0..meta_number {
            let meta = BlockMeta::read_from_binary(file)?;
            metas.push(meta);
        }
        Ok(SStableBlockMeta { block_metas: metas })
    }
    pub fn from_file(mut file: File) -> Result<Self> {
        let sstable_metas = SSTable::get_meta_from_file(&mut file)?;
        Ok(SSTable {
            sstable_metas: Arc::new(sstable_metas),
            file: RefCell::new(file),
        })
    }
    pub fn from(sstable_metas: Arc<SStableBlockMeta>, file: File) -> Result<Self> {
        Ok(SSTable {
            sstable_metas,
            file: RefCell::new(file),
        })
    }

    pub fn block_metadata(&self) -> Arc<SStableBlockMeta> {
        self.sstable_metas.clone()
    }

    pub fn start_key(&self) -> &Key {
        self.sstable_metas.block_metas.first().unwrap().start_key()
    }
    pub fn last_key(&self) -> &Key {
        self.sstable_metas.block_metas.last().unwrap().last_key()
    }
    pub fn get(&self, key: &Key) -> Result<Option<Value>> {
        if self.last_key().lt(key) {
            return Ok(None);
        }
        let block_position = self
            .sstable_metas
            .block_metas
            .partition_point(|meta| meta.last_key().lt(key));
        let block = self.read_block(block_position)?;
        let block_meta = &self.sstable_metas.block_metas[block_position];
        block.find(key, block_meta.entry_size())
    }

    pub fn entry_number(&self) -> usize {
        let mut res = 0;
        for meta in &self.sstable_metas.block_metas {
            res += meta.entry_number();
        }
        res
    }

    fn read_block(&self, block_position: usize) -> Result<Block> {
        let block_meta = &self.sstable_metas.block_metas[block_position];
        let mut read_ref = self.file.borrow_mut();
        read_ref.seek(Start(block_meta.block_offset()))?;
        let data_size = block_meta.size();
        assert!(data_size < BLOCK_POOL_MEMORY_SIZE);
        let mut data = [0; BLOCK_POOL_MEMORY_SIZE];
        read_ref.read_exact(&mut data[..data_size])?;
        let block = Block::new(data, data_size);
        Ok(block)
    }
    /// build new sstable, may not use out iterator if sstable size reach limit
    /// use BufWriter if possible
    /// return bool: if is finished
    pub fn from_iter(
        kv_iters: &mut dyn Iterator<Item = KVIterItem>,
        file: File,
    ) -> Result<(Option<SSTable>, bool)> {
        Self::from_iter_with_file_limit(kv_iters, file, Self::SSTABLE_SIZE_LIMIT)
    }
    pub fn from_iter_with_file_limit(
        kv_iters: &mut dyn Iterator<Item = KVIterItem>,
        mut file: File,
        limit_file_size: usize,
    ) -> Result<(Option<SSTable>, bool)> {
        let mut block_builder = BlockBuilder::new();
        let mut entry_count = 0;
        let mut block_metas = Vec::new();
        let mut last_block_position = 0;
        let mut start_key = None;
        let sstable_writer = &mut file;
        let iter_has_next;

        let mut next_entry = kv_iters.next();
        if next_entry.is_none() {
            return Ok((None, false));
        }
        loop {
            match next_entry {
                Some((key_slice, value)) => {
                    //     write to block_build
                    block_builder.append(key_slice, value)?;
                    entry_count += 1;

                    if start_key == None {
                        unsafe {
                            start_key = Some(Key::from(key_slice.data()));
                        }
                    }

                    next_entry = kv_iters.next();
                    //     check block_builder size, if is more than 4k flush it
                    if block_builder.len() > BLOCK_SIZE || next_entry.is_none() {
                        unsafe {
                            assert!(start_key.is_some());
                            block_metas.push(BlockMeta::new(
                                start_key.unwrap(),
                                Key::from(key_slice.data()),
                                entry_count,
                                block_builder.len(),
                                last_block_position,
                            ));
                        }
                        start_key = None;
                        last_block_position += block_builder.len() as u64;
                        block_builder.flush(sstable_writer)?;
                        entry_count = 0;
                    }
                }
                None => {
                    iter_has_next = false;
                    break;
                }
            }
            if limit_file_size > 0 && last_block_position >= limit_file_size as u64 {
                iter_has_next = true;
                info!("sstable size is {:}, reach file limit", last_block_position);
                break;
            }
        }

        // write block meta
        for block_meta in &block_metas {
            block_meta.write_to_binary(sstable_writer)?;
        }
        // write block meta number
        sstable_writer.write_u64::<LittleEndian>(block_metas.len() as u64)?;
        sstable_writer.write_u64::<LittleEndian>(last_block_position)?;

        Ok((
            Some(SSTable {
                sstable_metas: Arc::new(SStableBlockMeta { block_metas }),
                file: RefCell::new(file),
            }),
            iter_has_next,
        ))
    }

    pub fn iter(&self) -> Result<SStableIter> {
        SStableIter::new(self)
    }
}

impl Display for SSTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let iter = self.iter().unwrap();
        let mut res = String::new();
        for i in iter {
            let k: KeySlice = i.0;
            let v: ValueSliceTag = i.1;
            let v_string = if v.is_none() {
                String::from("None")
            } else {
                format!("{}", v.unwrap())
            };
            let display = format!("(key: {},value: {})", k, v_string);
            res.push_str(&display);
        }
        write!(f, "{}", res)
    }
}

impl SStableBlockMeta {
    pub fn last_key(&self) -> Key {
        let last_meta = self.block_metas.last().expect("wouldn't be empty");
        last_meta.last_key().clone()
    }

    pub fn first_key(&self) -> Key {
        let last_meta = self.block_metas.first().expect("wouldn't be empty");
        last_meta.start_key().clone()
    }
}

#[cfg(test)]
pub mod test {
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::{Cursor, Seek, SeekFrom};
    use std::str::from_utf8;
    use std::sync::Arc;

    use anyhow::Result;
    use log::LevelFilter;
    use tempfile::tempdir;

    use crate::db::common::{SortedKVIter, ValueWithTag};
    use crate::db::file_storage::FileStorageManager;
    use crate::db::key::{Key, KeySlice};
    use crate::db::sstable::SSTable;
    use crate::db::value::{Value, ValueSlice};

    pub fn build_sstable_with_special_value(
        start_number: usize,
        end_number: usize,
        step: usize,
        manual_set_value: HashMap<usize, ValueWithTag>,
        file: File,
    ) -> SSTable {
        let (data, output) = create_data(start_number, end_number, step, manual_set_value);

        let mut it = data.iter().map(|e| {
            (
                KeySlice::new(e.0.data()),
                e.1.as_ref().map(|f| ValueSlice::new(f.data())),
            )
        });
        let (sstable, _) = SSTable::from_iter(&mut it, file).unwrap();
        sstable.unwrap()
    }

    // build sstable 1->100
    pub fn build_sstable(
        start_number: usize,
        end_number: usize,
        step: usize,
        file: File,
    ) -> SSTable {
        build_sstable_with_special_value(start_number, end_number, step, HashMap::new(), file)
    }

    fn create_data(
        start_number: usize,
        end_number: usize,
        step: usize,
        manual_set_value: HashMap<usize, ValueWithTag>,
    ) -> (Vec<(Key, ValueWithTag)>, Vec<u8>) {
        let mut data = Vec::new();
        for i in (start_number..end_number).step_by(step) {
            if manual_set_value.contains_key(&i) {
                data.push((
                    Key::new(&i.to_string()),
                    manual_set_value.get(&i).unwrap().clone(),
                ));
            } else {
                data.push((Key::new(&i.to_string()), Some(Value::new(&i.to_string()))));
            }
        }
        let output: Vec<u8> = vec![0; 20 * (end_number - start_number) / step];
        (data, output)
    }

    #[test]
    fn test_build_sstable() {
        let mut data = Vec::new();
        let number = 100;
        for i in 0..number {
            data.push((Key::new(&i.to_string()), Value::new(&i.to_string())));
        }
        let output: Vec<u8> = vec![0; 20 * number];

        let mut it = data
            .iter()
            .map(|e| (KeySlice::new(e.0.data()), Some(ValueSlice::new(e.1.data()))));
        let c = tempfile::tempfile().unwrap();
        let (sstable, _) = SSTable::from_iter(&mut it, c).unwrap();
        let sstable = sstable.unwrap();

        // check sstable
        for i in 0..number {
            assert_eq!(
                sstable.get(&Key::new(&i.to_string())).unwrap().unwrap(),
                Value::new(&i.to_string())
            );
        }
    }

    #[test]
    fn test_stable_last_key_start_key() {
        let dir = tempdir().unwrap();
        let mut file_manager = FileStorageManager::new(dir.path());
        let (file, _, _) = file_manager.new_file().unwrap();
        let sstable = build_sstable(100, 200, 1, file);
        assert_eq!(sstable.last_key(), &Key::new("199"));
        assert_eq!(sstable.start_key(), &Key::new("100"));
    }

    #[test]
    fn test_stable_iter() {
        let dir = tempdir().unwrap();
        let mut file_manager = FileStorageManager::new(dir.path());
        let (file, _, _) = file_manager.new_file().unwrap();
        let sstable = build_sstable(0, 100, 1, file);
        let iter = sstable.iter().unwrap();
        for (i, kv) in iter.enumerate() {
            unsafe {
                let key = Key::from(kv.0.data());
                assert_eq!(key.to_string(), i.to_string());
            }
        }
    }

    #[test]
    fn test_build_sstable_on_file() {
        let dir = tempdir().unwrap();
        let mut file_manager = FileStorageManager::new(dir.path());
        let (file_1, file_1_id, _) = file_manager.new_file().unwrap();
        let sstable_1 = build_sstable(1, 10, 2, file_1);
        let mut sstable_1_iter = sstable_1.iter().unwrap();

        let (file_2, file_2_id, _) = file_manager.new_file().unwrap();
        let sstable_2 = build_sstable(0, 10, 2, file_2);
        let mut sstable_2_iter = sstable_2.iter().unwrap();

        let mut sorted_kv_iter = SortedKVIter::new(vec![&mut sstable_1_iter, &mut sstable_2_iter]);
        let sstable_3_file = tempfile::tempfile().unwrap();
        let (sstable_3, _) = SSTable::from_iter(&mut sorted_kv_iter, sstable_3_file).unwrap();
        let sstable_3 = sstable_3.unwrap();
        let sstable_3_on_file_iter = sstable_3.iter().unwrap();
        for (i, data) in sstable_3_on_file_iter.enumerate() {
            unsafe { assert_eq!(from_utf8(data.0.data()).unwrap(), i.to_string()) }
        }
    }

    #[test]
    fn test_sstable_display() {
        let dir = tempdir().unwrap();
        let mut file_manager = FileStorageManager::new(dir.path());
        let (file, _, _) = file_manager.new_file().unwrap();
        let sstable = build_sstable(1, 5, 1, file);
        assert_eq!(
            sstable.to_string(),
            "(key: 1,value: 1)(key: 2,value: 2)(key: 3,value: 3)(key: 4,value: 4)"
        );
    }

    #[test]
    fn test_get_sstable_meta() {
        let dir = tempdir().unwrap();
        let mut file_manager = FileStorageManager::new(dir.path());
        let (file, _, _) = file_manager.new_file().unwrap();
        let sstable_1 = build_sstable(1, 10, 2, file);
        let mut iter_1 = sstable_1.iter().unwrap();

        let dir = tempdir().unwrap();
        let path = dir.into_path();
        let mut file_manager = FileStorageManager::new(path.as_path());
        let (sstable_2_file, id, _) = file_manager.new_file().unwrap();
        let (sstable_2, _) = SSTable::from_iter(&mut iter_1, sstable_2_file).unwrap();
        let sstable_2 = sstable_2.unwrap();
        let sstable_2_meta = sstable_2.block_metadata();

        let mut file = FileStorageManager::open_file(path.as_path(), &id).unwrap();
        let meta = SSTable::get_meta_from_file(&mut file).unwrap();
        assert_eq!(format!("{:?}", meta), format!("{:?}", *sstable_2_meta))
    }

    #[test]
    fn test_stable_meta_last_key() {
        let dir = tempdir().unwrap();
        let mut file_manager = FileStorageManager::new(dir.path());
        let (file, _, _) = file_manager.new_file().unwrap();
        let sstable_1 = build_sstable(1, 10, 2, file);
        assert_eq!(sstable_1.sstable_metas.last_key(), Key::new("9"));
        assert_eq!(sstable_1.sstable_metas.first_key(), Key::new("1"));
    }
}
