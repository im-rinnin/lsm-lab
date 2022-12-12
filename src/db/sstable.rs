use std::cell::RefCell;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::io::SeekFrom::Start;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use byteorder::{LittleEndian, WriteBytesExt};
use log::info;

use crate::db::common::ValueSliceTag;
use crate::db::key::{Key, KEY_SIZE_LIMIT, KeySlice};
use crate::db::sstable::block::{Block, BLOCK_SIZE, BlockBuilder, BlockIter, BlockMeta};
use crate::db::value::{Value};

mod block;

const BLOCK_POOL_MEMORY_SIZE: usize = 2 * KEY_SIZE_LIMIT + BLOCK_SIZE;

/// format https://github.com/google/leveldb/blob/main/doc/table_format.md
/// block 1
/// block 2
///  ...
/// block n
/// block meta
/// block meta number (u64)
/// block meta offset (u64)
pub struct SSTable {
    sstable_metas: Arc<SStableMeta>,
    reader: Box<RefCell<dyn SSTableStorageReader>>,
}

pub struct SStableMeta {
    block_metas: Vec<BlockMeta>,
    block_metas_offset: u64,
}

pub struct FileBaseSSTable {
    path: PathBuf,
    sstable_meta: Arc<SStableMeta>,
}

pub trait SStableWriter: Write + Seek {
    fn as_write(&mut self) -> &mut dyn Write;
}

pub trait SSTableStorageReader: Read + Seek {
    fn as_reader(&mut self) -> &mut dyn Read;
}

struct WriterMetric<W: SStableWriter> {
    inner: W,
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
        Ok(SStableIter { block_iter, sstable, next_block_number: 1 })
    }
}

impl<'a> Iterator for SStableIter<'a> {
    type Item = (KeySlice, ValueSliceTag);
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

impl<W: SStableWriter> Write for WriterMetric<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl SSTable {
    pub const SSTABLE_SIZE_LIMIT: usize = 1024 * 1024 * 4;
    pub fn from(sstable_metas: Arc<SStableMeta>, store: Box<RefCell<dyn SSTableStorageReader>>) -> Result<Self> {
        Ok(SSTable { sstable_metas, reader: store })
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
        let block_position = self.sstable_metas.block_metas.partition_point(|meta| {
            meta.last_key().lt(key)
        });
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
        let mut read_ref = self.reader.borrow_mut();
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
    pub fn build(kv_iters: &mut dyn Iterator<Item=(KeySlice, ValueSliceTag)>,
                 sstable_writer: &mut dyn Write) -> Result<SStableMeta> {
        let mut block_builder = BlockBuilder::new();
        let mut entry_count = 0;
        let mut block_metas = Vec::with_capacity(Self::SSTABLE_SIZE_LIMIT / BLOCK_SIZE as usize);
        let mut last_block_position = 0;
        let mut start_key = None;

        let mut next_entry = kv_iters.next();
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
                            block_metas.push(BlockMeta::new(start_key.unwrap(), Key::from(key_slice.data()),
                                                            entry_count, block_builder.len(), last_block_position));
                        }
                        start_key = None;
                        last_block_position += block_builder.len() as u64;
                        block_builder.flush(sstable_writer)?;
                        entry_count = 0;
                    }
                }
                None => { break; }
            }
            if last_block_position >= Self::SSTABLE_SIZE_LIMIT as u64 {
                info!("sstable size is {:}, reach file limit",last_block_position);
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


        drop(sstable_writer);

        Ok(SStableMeta { block_metas, block_metas_offset: last_block_position })
    }


    fn block_metas_offset(&self) -> SeekFrom {
        SeekFrom::Start(self.sstable_metas.block_metas_offset)
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

impl FileBaseSSTable {
    pub fn new(sstable_meta: SStableMeta, path: PathBuf) -> Self {
        FileBaseSSTable { path, sstable_meta: Arc::new(sstable_meta) }
    }
    pub fn new_sstable(&self) -> Result<SSTable> {
        let file = Box::new(RefCell::new(File::open(self.path.as_path())?));
        let sstable_meta = self.sstable_meta.clone();
        SSTable::from(sstable_meta, file)
    }
}

impl SSTableStorageReader for File {
    fn as_reader(&mut self) -> &mut dyn Read {
        self
    }
}

impl SStableWriter for File {
    fn as_write(&mut self) -> &mut dyn Write {
        self
    }
}

impl SSTableStorageReader for Cursor<Vec<u8>> {
    fn as_reader(&mut self) -> &mut dyn Read {
        self
    }
}

impl SStableWriter for Cursor<Vec<u8>> {
    fn as_write(&mut self) -> &mut dyn Write {
        self
    }
}

#[cfg(test)]
pub mod test {
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::{Cursor};
    use std::str::from_utf8;
    use std::sync::Arc;

    use tempfile::tempdir;

    use crate::db::common::{SortedKVIter, ValueWithTag};
    use crate::db::file_storage::FileStorageManager;
    use crate::db::key::{Key, KeySlice};
    use crate::db::sstable::{FileBaseSSTable, SSTable, };
    use crate::db::value::{Value, ValueSlice};

    #[test]
    fn test_build_sstable() {
        let mut data = Vec::new();
        let number = 100;
        for i in 0..number {
            data.push((Key::new(&i.to_string()), Value::new(&i.to_string())));
        }
        let output: Vec<u8> = vec![0; 20 * number];

        let mut it = data.iter().map(|e| (KeySlice::new(e.0.data()),
                                          Some(ValueSlice::new(e.1.data()))));
        let mut c = Cursor::new(output);
        let sstable_metas = Arc::new(SSTable::build(&mut it, &mut c).unwrap());
        let sstable = SSTable::from(sstable_metas, Box::new(RefCell::new(c))).unwrap();

        // check sstable
        for i in 0..number {
            assert_eq!(sstable.get(&Key::new(&i.to_string())).unwrap().unwrap(), Value::new(&i.to_string()));
        }
    }

    pub fn build_sstable_with_special_value(start_number: usize, end_number: usize, step: usize, manual_set_value: HashMap<usize, ValueWithTag>) -> SSTable {
        let (data, output) = create_data(start_number, end_number, step, manual_set_value);

        let mut it = data.iter().map(|e| (KeySlice::new(e.0.data()),
                                          e.1.as_ref().map(|f| ValueSlice::new(f.data()))));
        let mut c = Cursor::new(output);
        let sstable_metas = Arc::new(SSTable::build(&mut it, &mut c).unwrap());
        let sstable = SSTable::from(sstable_metas, Box::new(RefCell::new(c))).unwrap();
        sstable
    }

    // build sstable 1->100
    pub fn build_sstable(start_number: usize, end_number: usize, step: usize) -> SSTable {
        build_sstable_with_special_value(start_number, end_number, step, HashMap::new())
    }

    fn create_data(start_number: usize, end_number: usize, step: usize, manual_set_value: HashMap<usize, ValueWithTag>) -> (Vec<(Key, ValueWithTag)>, Vec<u8>) {
        let mut data = Vec::new();
        for i in (start_number..end_number).step_by(step) {
            if manual_set_value.contains_key(&i) {
                data.push((Key::new(&i.to_string()), manual_set_value.get(&i).unwrap().clone()));
            } else {
                data.push((Key::new(&i.to_string()), Some(Value::new(&i.to_string()))));
            }
        }
        let output: Vec<u8> = vec![0; 20 * (end_number - start_number) / step];
        (data, output)
    }

    #[test]
    fn test_stable_last_key_start_key() {
        let sstable = build_sstable(100, 200, 1);
        assert_eq!(sstable.last_key(), &Key::new("199"));
        assert_eq!(sstable.start_key(), &Key::new("100"));
    }

    #[test]
    fn test_stable_iter() {
        let sstable = build_sstable(0, 100, 1);
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
        let sstable_1 = build_sstable(1, 10, 2);
        let mut sstable_1_iter = sstable_1.iter().unwrap();

        let sstable_2 = build_sstable(0, 10, 2);
        let mut iter_2 = sstable_2.iter().unwrap();
        let dir = tempdir().unwrap();
        let mut file_manager = FileStorageManager::new(dir.path());
        let mut sstable_2_file = file_manager.new_file().unwrap().0;
        let sstable_2_meta = Arc::new(SSTable::build(&mut iter_2, &mut sstable_2_file).unwrap());
        let sstable_2_on_file = SSTable::from(sstable_2_meta, Box::new(RefCell::new(sstable_2_file))).unwrap();
        let mut sstable_2_on_file_iter = sstable_2_on_file.iter().unwrap();

        let mut sorted_kv_iter = SortedKVIter::new(vec![&mut sstable_1_iter, &mut sstable_2_on_file_iter]);
        let mut sstable_3_file = tempfile::tempfile().unwrap();
        let sstable_3_meta = Arc::new(SSTable::build(&mut sorted_kv_iter, &mut sstable_3_file).unwrap());
        let sstable_3 = SSTable::from(sstable_3_meta, Box::new(RefCell::new(sstable_3_file))).unwrap();
        let sstable_3_on_file_iter = sstable_3.iter().unwrap();
        for (i, data) in sstable_3_on_file_iter.enumerate() {
            unsafe {
                assert_eq!(from_utf8(data.0.data()).unwrap(), i.to_string())
            }
        }
    }

    #[test]
    fn test_sstable_display() {
        let sstable = build_sstable(1, 5, 1);
        assert_eq!(sstable.to_string(), "(key: 1,value: 1)(key: 2,value: 2)(key: 3,value: 3)(key: 4,value: 4)");
    }

    #[test]
    fn test_file_sstable() {
        let dir = tempdir().unwrap();
        let path = dir.into_path().join("test");
        let mut file = File::create(path.as_path()).unwrap();

        let sstable = build_sstable(1, 10, 1);
        let mut iter = sstable.iter().unwrap();
        let sstable_2 = SSTable::build(&mut iter, &mut file).unwrap();

        let file_sstable = FileBaseSSTable::new(sstable_2, path);
        let sstable_2_clone = file_sstable.new_sstable().unwrap();
        assert_eq!(sstable.to_string(), sstable_2_clone.to_string());
    }
}