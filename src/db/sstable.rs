use std::cell::RefCell;
use std::fs::File;
use std::io::{BufWriter, Cursor, Read, Seek, SeekFrom, Write};
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
    sstable_metas: SStableMeta,
    reader: Box<RefCell<dyn SSTableReader>>,
}

pub struct SStableMeta {
    block_metas: Vec<BlockMeta>,
    block_metas_offset: u64,
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
    pub fn from(sstable_metas: SStableMeta, store: Box<RefCell<dyn SSTableReader>>) -> Result<Self> {
        Ok(SSTable { sstable_metas, reader: store })
    }
    pub fn new(store: Box<RefCell<dyn SSTableReader>>) -> Result<Self> {
        let mut reader_ref = store.borrow_mut();
        // block meta number (u64)
        // block meta offset (u64)
        // 8+8=16
        reader_ref.seek(SeekFrom::End(-16))?;
        let block_metas_number = reader_ref.as_reader().read_u64::<LittleEndian>()?;
        let block_metas_offset = reader_ref.as_reader().read_u64::<LittleEndian>()?;
        reader_ref.seek(SeekFrom::Start(block_metas_offset))?;
        let block_metas = BlockMeta::build_block_metas(&mut *reader_ref.as_reader(), block_metas_number as usize)?;
        drop(reader_ref);
        Ok(SSTable { sstable_metas: SStableMeta { block_metas, block_metas_offset }, reader: store })
    }
    pub fn last_key(&self) -> &Key {
        self.sstable_metas.block_metas.last().unwrap().last_key()
    }
    pub fn get(&self, key: &Key) -> Result<Option<Value>> {
        assert!(self.last_key().ge(key));
        let block_position = self.sstable_metas.block_metas.partition_point(|meta| {
            meta.last_key().lt(key)
        });
        let block_meta = &self.sstable_metas.block_metas[block_position];
        let mut read_ref = self.reader.borrow_mut();
        read_ref.seek(Start(block_meta.block_offset()))?;
        let mut data = vec![0; block_meta.size()];
        read_ref.read_exact(&mut data)?;
        let block = Block::new(data);
        block.find(key, block_meta.entry_size())
    }
    // build new sstable
    pub fn build(kv_iters: &mut dyn Iterator<Item=(KeySlice, ValueSliceTag)>,
                 sstable_writer: &mut dyn SStableWriter) -> Result<SStableMeta> {
        let mut block_builder = BlockBuilder::new();
        let mut entry_count = 0;
        let mut block_metas = Vec::new();
        let mut last_block_position = 0;

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
                        last_block_position += block_builder.len() as u64;
                        block_builder.flush(sstable_writer.as_write())?;
                        entry_count = 0;
                    }
                }
                None => { break; }
            }
        }

        // write block meta
        for block_meta in &block_metas {
            block_meta.write_to_binary(sstable_writer.as_write())?;
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
}

impl SSTableReader for File {
    fn as_reader(&mut self) -> &mut dyn Read {
        self
    }
}

impl SStableWriter for File {
    fn as_write(&mut self) -> &mut dyn Write {
        self
    }
}

impl SSTableReader for Cursor<Vec<u8>> {
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
        let mut output: Vec<u8> = vec![0; 20 * number];

        let mut it = data.iter().map(|e| (KeySlice::new(e.0.data()),
                                          Some(ValueSlice::new(e.1.data()))));
        let mut c = Cursor::new(output);
        let sstable_metas = SSTable::build(&mut it, &mut c).unwrap();
        let sstable = SSTable::from(sstable_metas, Box::new(RefCell::new(c))).unwrap();

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