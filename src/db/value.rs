use core::slice;

#[derive(Clone, Eq, PartialEq, Debug, Ord, PartialOrd)]
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
        ValueSlice { ptr: v.as_ptr(), size: v.len() }
    }
    pub fn len(&self) -> usize {
        self.size
    }
    pub unsafe fn data(&self) -> &[u8] {
        slice::from_raw_parts(self.ptr, self.size)
    }
}

const VALUE_SIZE_LIMIT: usize = 1024;

impl Value {
    pub fn from_u8(s: &[u8]) -> Self {
        Value { v: Vec::from(s) }
    }
    pub fn new(s: &str) -> Self {
        assert!(s.len() < VALUE_SIZE_LIMIT);
        Value { v: Vec::from(s.as_bytes()) }
    }
    pub fn data(&self) -> &[u8] {
        self.v.as_slice()
    }

    pub fn len(&self) -> usize {
        self.data().len()
    }
}
