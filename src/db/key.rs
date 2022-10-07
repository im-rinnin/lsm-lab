
#[derive(Clone,Eq,PartialEq,PartialOrd,Ord)]
pub struct Key {
    k: String,
}

impl Key {
    pub fn new(s:&String)->Self{
        Key { k: s.clone() }
    }

    pub fn data(&self)->&[u8]{
        self.k.as_bytes()
    }
}


