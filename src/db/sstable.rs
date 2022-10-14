use std::cell::RefCell;
use std::fs::File;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::io::SeekFrom::Start;

use anyhow::Result;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::db::common::{ValueSliceTag, ValueWithTag};
use crate::db::key::{Key, KeySlice};
use crate::db::sstable::block::{Block, BLOCK_SIZE, BlockBuilder, BlockMeta};
use crate::db::value::Value;

mod block;

/// format https://github.com/google/leveldb/blob/main/doc/table_format.md
/// block 1
/// block 2
///  ...
/// block n
/// block meta
/// block meta number (u64)
/// block meta offset (u64)
pub struct SSTable {
    block_metas: Vec<BlockMeta>,
    reader: Box<RefCell<dyn SStableStore>>,
    block_metas_offset: u64,
}

pub trait SStableStore: Write + Seek + Read {
    fn len(&self) -> u64;
    fn as_write(&mut self) -> &mut dyn Write;
    fn as_reader(&mut self) -> &mut dyn Read;
}

pub trait SStableWriter: Write + Seek {
    fn as_write(&mut self) -> &mut dyn Write;
}

pub trait SSTableReader: Read + Seek {
    fn as_reader(&mut self) -> &mut dyn Read;
}

struct WriterMetric<W: SStableWriter> {
    inner: W,
}

struct SStableIter<'a> {
    sstable: &'a SSTable,
    next_block_offset: usize,
    current_block: Block,
    current_block_meta_index: usize,
}

impl<'a> Iterator for SStableIter<'a> {
    type Item = (&'a Key, &'a ValueWithTag);

    fn next(&mut self) -> Option<Self::Item> {
        // if self.next_position == self.sstable.block_metas_offset {
        //     return None;
        // }
        // self.sstable.get_by_seek(self.next_position) -> ;
        todo!()
    }
}

impl<W: SStableWriter> Write for WriterMetric<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        metrics::counter!("sstable.writer", buf.len() as u64);
        self.inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl SSTable {
    pub fn new(reader: Box<RefCell<dyn SStableStore>>) -> Result<Self> {
        let mut reader_ref = reader.borrow_mut();
        // block meta number (u64)
        // block meta offset (u64)
        // 8+8=16
        reader_ref.seek(SeekFrom::End(-16))?;
        let block_metas_number = reader_ref.as_reader().read_u64::<LittleEndian>()?;
        let block_metas_offset = reader_ref.as_reader().read_u64::<LittleEndian>()?;
        reader_ref.seek(SeekFrom::Start(block_metas_offset))?;
        let metas = BlockMeta::build_block_metas(&mut *reader_ref.as_reader(), block_metas_number as usize)?;
        drop(reader_ref);
        Ok(SSTable { block_metas: metas, reader, block_metas_offset })
    }
    pub fn last_key(&self) -> &Key {
        self.block_metas.last().unwrap().last_key()
    }
    pub fn get(&self, key: &Key) -> Result<Option<Value>> {
        assert!(self.last_key().ge(key));
        let block_position = self.block_metas.partition_point(|meta| {
            meta.last_key().lt(key)
        });
        let block_meta = &self.block_metas[block_position];
        let mut read_ref = self.reader.borrow_mut();
        read_ref.seek(Start(block_meta.block_offset()))?;
        let mut data = vec![0; block_meta.size()];
        read_ref.read_exact(&mut data)?;
        let block = Block::new(data);
        block.find(key, block_meta.entry_size())
    }
    // build new sstable
    pub fn build(kv_iters: &mut dyn Iterator<Item=(KeySlice, ValueSliceTag)>,
                 mut sstable_store: Box<RefCell<dyn SStableStore>>) -> Result<SSTable> {
        let mut block_builder = BlockBuilder::new();
        let mut entry_count = 0;
        let mut block_metas = Vec::new();
        let mut last_block_position = 0;
        let mut read_and_write = sstable_store.borrow_mut();


        let mut next_entry = kv_iters.next();
        loop {
            match next_entry {
                Some((key_slice, value)) => {
                    //     write to block_build
                    block_builder.append(key_slice, value)?;
                    entry_count += 1;

                    next_entry = kv_iters.next();
                    //     check block_builder size, if is more than 4k flush it
                    if block_builder.len() > BLOCK_SIZE || next_entry.is_none() {
                        unsafe {
                            block_metas.push(BlockMeta::new(Key::from(key_slice.data()), entry_count, block_builder.len(), last_block_position));
                        }
                        block_builder.flush(read_and_write.as_write())?;
                        entry_count = 0;
                        last_block_position = read_and_write.stream_position()?;
                    }
                }
                None => { break; }
            }
        }

        // write block meta
        let block_metas_offset = read_and_write.stream_position()?;
        for block_meta in &block_metas {
            block_meta.write_to_binary(read_and_write.as_write())?;
        }
        // write block meta number
        read_and_write.write_u64::<LittleEndian>(block_metas.len() as u64)?;
        read_and_write.write_u64::<LittleEndian>(block_metas_offset)?;
        drop(read_and_write);
        Ok(SSTable { block_metas, reader: sstable_store, block_metas_offset })
    }

    fn block_metas_offset(&self) -> SeekFrom {
        SeekFrom::Start(self.block_metas_offset)
    }
}

impl SSTableReader for File {
    fn as_reader(&mut self) -> &mut dyn Read {
        self
    }
}

impl SStableStore for File {
    fn len(&self) -> u64 {
        self.metadata().unwrap().len()
    }

    fn as_write(&mut self) -> &mut dyn Write {
        self
    }

    fn as_reader(&mut self) -> &mut dyn Read {
        self
    }
}

// use cursor for test
impl SStableStore for Cursor<Vec<u8>> {
    fn len(&self) -> u64 {
        self.get_ref().len() as u64
    }

    fn as_write(&mut self) -> &mut dyn Write {
        self
    }

    fn as_reader(&mut self) -> &mut dyn Read {
        self
    }
}

impl SSTableReader for Cursor<Vec<u8>> {
    fn as_reader(&mut self) -> &mut dyn Read {
        self
    }
}

impl SStableWriter for Cursor<&mut [u8]> {
    fn as_write(&mut self) -> &mut dyn Write {
        self
    }
}


#[cfg(test)]
mod test {
    use std::cell::RefCell;
    use std::io::{Cursor, Seek};

    use crate::db::key::{Key, KeySlice};
    use crate::db::sstable::SSTable;
    use crate::db::value::{Value, ValueSlice};

    #[test]
    fn test_build_sstable() {
        let mut data = Vec::new();
        let number = 100;
        for i in 0..number {
            data.push((Key::new(&i.to_string()), Value::new(&i.to_string())));
        }
        let mut output = vec![0; 20 * number];

        let mut it = data.iter().map(|e| (KeySlice::new(e.0.data()),
                                          Some(ValueSlice::new(e.1.data()))));
        let mut c = Cursor::new(output);
        let sstable = SSTable::build(&mut it, Box::new(RefCell::new(c))).unwrap();

        // check sstable
        for i in 0..number {
            assert_eq!(sstable.get(&Key::new(&i.to_string())).unwrap().unwrap(), Value::new(&i.to_string()));
        }
    }

    #[test]
    fn test_build_sstable_on_file() {
        //     todo
    }
}