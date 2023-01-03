use core::slice;
use std::fmt::{Display, Formatter};
use std::slice::from_raw_parts;

use serde::{Serialize, Deserialize};

#[derive(Clone, Eq, PartialEq, Debug, Ord, PartialOrd, Serialize, Deserialize)]
pub struct Value {
    v: Vec<u8>,
}

#[derive(Clone, Copy, Eq, PartialEq, Debug, Ord, PartialOrd)]
pub struct ValueSlice {
    ptr: *const u8,
    size: usize,
}

impl ValueSlice {
    pub fn new(v: &[u8]) -> Self {
        ValueSlice {
            ptr: v.as_ptr(),
            size: v.len(),
        }
    }
    pub fn len(&self) -> usize {
        self.size
    }
    pub unsafe fn data(&self) -> &[u8] {
        slice::from_raw_parts(self.ptr, self.size)
    }
}

impl Display for ValueSlice {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        unsafe {
            let a = from_raw_parts(self.ptr, self.size);
            let res = std::str::from_utf8_unchecked(a);
            write!(f, "{}", res)
        }
    }
}

pub const VALUE_SIZE_LIMIT: usize = 1024;

impl Value {
    pub fn from_u8(s: &[u8]) -> Self {
        Value { v: Vec::from(s) }
    }
    pub fn new(s: &str) -> Self {
        assert!(s.len() < VALUE_SIZE_LIMIT);
        Value {
            v: Vec::from(s.as_bytes()),
        }
    }
    pub fn data(&self) -> &[u8] {
        self.v.as_slice()
    }

    pub fn len(&self) -> usize {
        self.data().len()
    }
}

#[cfg(test)]
mod test {
    use crate::db::value::{Value, ValueSlice};

    #[test]
    fn test_display() {
        let v = Value::new("123");
        let v_slice = ValueSlice::new(v.data());
        assert_eq!(v_slice.to_string(), "123");
    }
}
