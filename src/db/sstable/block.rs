use std::io::{Read, Write};

use anyhow::Result;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::db::common::ValueSliceTag;
use crate::db::key::{Key, KeySlice};
use crate::db::sstable::SSTableReader;
use crate::db::value::{Value, ValueSlice};

pub const BLOCK_SIZE: usize = 4 * 1024 * 1024;

/// entry format
/// [key size(u16),key data,value size(u16),value data]
pub struct Block {
    content: Vec<u8>,
}

/// data block,4k default
/// entry 1
/// entry 2
/// ...
/// entry n
/// pad
pub struct BlockBuilder {
    content: Vec<u8>,
}

/// [last_key offset, block_size u16,entry_number u16]
pub struct BlockMeta {
    last_key: Key,
    block_offset: u64,
    size: usize,
    entry_number: usize,
}

pub struct BlockIter<'a> {
    block: &'a Block,
    next_position: usize,
}

impl Block {
    const SIZE_LEN: usize = 2;
    pub fn new(v: Vec<u8>) -> Self {
        Block { content: v }
    }

    pub fn find(&self, key: &Key, entry_number: usize) -> Result<Option<Value>> {
        let mut position = 0;
        let mut count = 0;
        while count < entry_number {
            count += 1;
            let (key_content, value_content) = self.read_kv_at(&mut position)?;

            if key.equal_u8(key_content) && value_content.is_some() {
                return Ok(Some(Value::from_u8(value_content.unwrap())));
            }
        }
        Ok(None)
    }

    // value is none if is deleted
    fn read_kv_at(&self, mut position: &mut usize) -> Result<(&[u8], Option<&[u8]>)> {
        let key_size = (&self.content[*position..*position + Self::SIZE_LEN]).read_u16::<LittleEndian>()? as usize;
        *position += Self::SIZE_LEN;

        let key_content = &self.content[*position..*position + key_size];
        *position += key_size;
        let value_size = (&self.content[*position..*position + Self::SIZE_LEN]).read_u16::<LittleEndian>()? as usize;
        if value_size > 0 {
            *position += Self::SIZE_LEN;
            let value_content = &self.content[*position..*position + value_size];
            *position += value_size;
            Ok((key_content, Some(value_content)))
        } else {
            *position += Self::SIZE_LEN;
            Ok((key_content, None))
        }
    }

    pub fn iter(&self) -> BlockIter {
        BlockIter { block: self, next_position: 0 }
    }
}

impl<'a> Iterator for BlockIter<'a> {
    type Item = (KeySlice, ValueSliceTag);

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_position == self.block.content.len() {
            return None;
        }
        let (k, v) = self.block.read_kv_at(&mut self.next_position).unwrap();
        let next_key_slice = KeySlice::new(k);
        let next_value_slice = v.map(|data| ValueSlice::new(data));
        Some((next_key_slice, next_value_slice))
    }
}

impl BlockMeta {
    pub fn last_key(&self) -> &Key {
        &self.last_key
    }
    pub fn block_offset(&self) -> u64 {
        self.block_offset
    }
    pub fn entry_size(&self) -> usize {
        self.entry_number
    }
    pub fn size(&self) -> usize {
        self.size
    }
    pub fn new(k: Key, number: usize, size: usize, block_offset: u64) -> Self {
        BlockMeta { last_key: k, entry_number: number, size, block_offset }
    }

    // [key_size,key_content,entry_number]
    pub fn write_to_binary(&self, write: &mut dyn Write) -> Result<()> {
        write.write_u16::<LittleEndian>(self.last_key.len() as u16)?;
        write.write(self.last_key.data())?;
        write.write_u16::<LittleEndian>(self.block_offset as u16)?;
        write.write_u16::<LittleEndian>(self.size as u16)?;
        write.write_u16::<LittleEndian>(self.entry_number as u16)?;
        Ok(())
    }

    pub fn build_block_metas(data: &mut dyn Read, number: usize) -> Result<Vec<BlockMeta>> {
        let mut count = 0;
        let mut result = Vec::new();
        // let mut position = 0;
        while count < number {
            count += 1;
            let key_size = data.read_u16::<LittleEndian>()? as usize;

            let mut key_data = vec![0; key_size];
            data.read_exact(&mut key_data)?;

            let last_key = Key::from_u8_vec(key_data);
            let block_offset = data.read_u16::<LittleEndian>()?;
            let size = data.read_u16::<LittleEndian>()?;
            let entry_number = data.read_u16::<LittleEndian>()?;

            result.push(BlockMeta::new(last_key, entry_number as usize, size as usize, block_offset as u64));
        }
        Ok(result)
    }
}

impl BlockBuilder {
    pub fn new() -> Self {
        BlockBuilder { content: Vec::new() }
    }

    pub fn len(&self) -> usize {
        self.content.len()
    }

    pub fn append(&mut self, key: KeySlice, value_with_tag: ValueSliceTag) -> Result<()> {
        self.content.write_u16::<LittleEndian>(key.len() as u16)?;
        unsafe {
            self.content.write(key.data())?;
        }

        if let Some(valueRef) = value_with_tag {
            self.content.write_u16::<LittleEndian>(valueRef.len() as u16)?;
            unsafe { self.content.write(valueRef.data())?; }
        } else {
            self.content.write_u16::<LittleEndian>(0)?;
        }
        Ok(())
    }

    pub fn flush(&mut self, w: &mut dyn Write) -> Result<()> {
        w.write(self.content.as_slice())?;
        self.content.clear();
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use crate::db::key::Key;
    use crate::db::key::KeySlice;
    use crate::db::sstable::block::{Block, BlockBuilder, BlockMeta};
    use crate::db::value::{Value, ValueSlice};

    #[test]
    fn test_block_builder_and_read() {
        let data = vec![(1, false), (2, false), (3, true), (6, false), (7, false)];
        let block = create_block(&data);

        let number = data.len();
        for (key, is_deleted) in data.iter() {
            let res = block.find(&Key::new(&key.to_string()), number).unwrap();
            if *is_deleted {
                assert!(res.is_none());
            } else {
                assert_eq!(res.unwrap(), Value::new(&key.to_string()))
            }
        }
    }

    // true if is deleted
    fn create_block(input: &Vec<(u32, bool)>) -> Block {
        let mut b_builder = BlockBuilder::new();
        let mut content: Vec<u8> = Vec::new();
        for (number, is_deleted) in input {
            let number_string = number.to_string();
            let number_slice = number_string.as_bytes();
            let key_slice = KeySlice::new(number_slice);
            let value_slice = if *is_deleted {
                None
            } else {
                Some(ValueSlice::new(number_slice))
            };
            b_builder.append(KeySlice::new(number.to_string().as_bytes()),
                             value_slice).unwrap();
        }
        b_builder.flush(&mut content).unwrap();
        assert_eq!(b_builder.len(), 0);
        Block::new(content)
    }

    #[test]
    fn test_block_meta_builder_and_read() {
        let mut content = Vec::new();
        let b1 = BlockMeta::new(Key::new("a"), 10, 1, 0);
        let b2 = BlockMeta::new(Key::new("b"), 5, 2, 100);
        b1.write_to_binary(&mut content).unwrap();
        b2.write_to_binary(&mut content).unwrap();

        let block_metas = BlockMeta::build_block_metas(&mut Cursor::new(content), 2).unwrap();
        assert_eq!(block_metas[0].last_key(), &Key::new("a"));
        assert_eq!(block_metas[0].entry_size(), 10);
        assert_eq!(block_metas[0].size(), 1);
        assert_eq!(block_metas[0].block_offset(), 0);
        assert_eq!(block_metas[1].last_key(), &Key::new("b"));
        assert_eq!(block_metas[1].size(), 2);
        assert_eq!(block_metas[1].block_offset(), 100);
        assert_eq!(block_metas[1].entry_size(), 5);
    }

    #[test]
    fn test_block_iter() {
        let data = vec![(1, false), (2, false), (3, true), (6, false), (7, false)];
        let block = create_block(&data);
        let block_iter = block.iter();
        let mut res = Vec::new();
        for (key, value) in block_iter {
            if value.is_some() {
                unsafe {
                    assert_eq!(key.data(), value.unwrap().data());
                }
                res.push((key, false));
            } else {
                res.push((key, true));
            }
        }
        for (i, key) in data.iter().enumerate() {
            unsafe {
                assert_eq!(res[i].0.data(), key.0.to_string().as_bytes())
            }
        }
    }
}