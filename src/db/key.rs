use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::slice::from_raw_parts;

#[derive(Clone, Eq, PartialEq, PartialOrd, Ord, Debug, Hash)]
pub struct Key {
    k: String,
}


#[derive(Clone, Eq, Copy, Debug)]
pub struct KeySlice {
    ptr: *const u8,
    size: usize,
}

impl Display for KeySlice {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        unsafe {
            let a = from_raw_parts(self.ptr, self.size);
            let res = std::str::from_utf8_unchecked(a);
            write!(f, "{}", res)
        }
    }
}

impl Ord for KeySlice {
    fn cmp(&self, other: &Self) -> Ordering {
        unsafe {
            let a = from_raw_parts(self.ptr, self.size);
            let b = from_raw_parts(other.ptr, other.size);
            a.cmp(&b)
        }
    }
}

impl PartialOrd for KeySlice {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        unsafe {
            let a = from_raw_parts(self.ptr, self.size);
            let b = from_raw_parts(other.ptr, other.size);
            a.partial_cmp(&b)
        }
    }
}

impl PartialEq for KeySlice {
    fn eq(&self, other: &Self) -> bool {
        unsafe {
            let a = from_raw_parts(self.ptr, self.size);
            let b = from_raw_parts(other.ptr, other.size);
            a.eq(b)
        }
    }
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

#[cfg(test)]
mod test {
    use std::cmp::Ordering;

    use crate::db::key::{Key, KeySlice};

    #[test]
    pub fn test_key_slice_compare() {
        let a = Key::new("abc");
        let b = Key::new("abd");

        let a_key_slice = KeySlice::new(a.data());
        let b_key_slice = KeySlice::new(b.data());

        assert!(!a_key_slice.eq(&b_key_slice));
        assert_eq!(a_key_slice.cmp(&b_key_slice), Ordering::Less);

        let a = Key::new("abc");
        let b = Key::new("abc");


        let a_key_slice = KeySlice::new(a.data());
        let b_key_slice = KeySlice::new(b.data());

        assert!(a_key_slice.eq(&b_key_slice));
        assert_eq!(a_key_slice.cmp(&b_key_slice), Ordering::Equal);

        let a = Key::new("bc");
        let b = Key::new("abc");


        let a_key_slice = KeySlice::new(a.data());
        let b_key_slice = KeySlice::new(b.data());

        assert!(!a_key_slice.eq(&b_key_slice));
        assert_eq!(a_key_slice.cmp(&b_key_slice), Ordering::Greater);
    }
    #[test]
    pub fn test_key_slice_display() {
        let key = Key::new("123");
        let key_slice=KeySlice::new(key.data());
        assert_eq!(key_slice.to_string(), "123");
    }
}


