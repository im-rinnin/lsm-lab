use std::slice::from_raw_parts;

#[derive(Clone, Eq, PartialEq, PartialOrd, Ord, Debug, Hash)]
pub struct Key {
    k: String,
}


#[derive(Clone, Copy, Eq, PartialEq, PartialOrd, Ord, Debug, Hash)]
pub struct KeySlice {
    ptr: *const u8,
    size: usize,
}

pub const KEY_SIZE_LIMIT: usize = 1024;

impl KeySlice {
    pub fn new(data: &[u8]) -> Self {
        KeySlice { ptr: data.as_ptr(), size: data.len() }
    }
    pub fn len(&self) -> usize {
        self.size
    }
    pub unsafe fn data(&self) -> &[u8] {
        from_raw_parts(self.ptr, self.size)
    }
}

impl Key {
    pub fn new(s: &str) -> Self {
        assert!(s.len() < KEY_SIZE_LIMIT);
        Key { k: s.to_string() }
    }
    pub fn from_u32(i: u32) -> Self {
        Self::new(&i.to_string())
    }

    pub fn from(s: &[u8]) -> Self {
        assert!(s.len() < KEY_SIZE_LIMIT);
        Key { k: String::from_utf8(s.to_vec()).unwrap() }
    }

    pub fn from_u8_vec(v: Vec<u8>) -> Self {
        assert!(v.len() < KEY_SIZE_LIMIT);
        Key { k: String::from_utf8(v).unwrap() }
    }

    pub fn to_string(&self) -> &str {
        &self.k
    }

    pub fn data(&self) -> &[u8] {
        self.k.as_bytes()
    }

    pub fn len(&self) -> usize {
        self.k.as_bytes().len()
    }

    pub fn equal_u8(&self, data: &[u8]) -> bool {
        self.data().eq(data)
    }
}


