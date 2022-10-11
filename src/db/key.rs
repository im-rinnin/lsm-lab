
#[derive(Clone, Eq, PartialEq, PartialOrd, Ord, Debug)]
pub struct Key {
    k: String,
}

const KEY_SIZE_LIMIT: usize = 1024;

impl Key {
    pub fn new(s: &str) -> Self {
        assert!(s.len() < KEY_SIZE_LIMIT);
        Key { k: s.to_string() }
    }
    pub fn from(s: &[u8]) -> Self {
        assert!(s.len() < KEY_SIZE_LIMIT);
        Key { k: String::from_utf8(s.to_vec()).unwrap() }
    }

    pub fn from_u8_vec(v: Vec<u8>) -> Self {
        assert!(v.len() < KEY_SIZE_LIMIT);
        Key { k: String::from_utf8(v).unwrap() }
    }

    pub fn to_string(&self)->&str{
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


