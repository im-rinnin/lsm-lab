use super::{key::Key, value::Value};
use anyhow::Result;

pub enum Operation {
    PUT { key: Key, value: Value },
    DELETE { key: Key },
}
pub struct WriteBatch {
    ops: Vec<Operation>,
}

impl WriteBatch {
    pub fn new() -> Self {
        WriteBatch { ops: Vec::new() }
    }
    pub fn put(&mut self, key: Key, value: Value) {
        self.ops.push(Operation::PUT {
            key: key,
            value: value,
        });
    }
    pub fn delete(&mut self, key: Key) {
        self.ops.push(Operation::DELETE { key: key });
    }
    pub fn to_opertions(&self) -> &Vec<Operation> {
        &self.ops
    }
    pub fn size(&self) -> usize {
        let mut res = 0;
        for entry in &self.ops {
            match entry {
                Operation::PUT { key, value } => {
                    res += key.len() + value.len();
                }
                Operation::DELETE { key } => {
                    res += key.len();
                }
            }
        }
        res
    }
}

#[cfg(test)]
mod test {}
