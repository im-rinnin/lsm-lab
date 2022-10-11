#[derive(Clone,Eq,PartialEq,Debug)]
pub struct Value {
    v: Vec<u8>,
}

const VALUE_SIZE_LIMIT: usize = 1024;


impl Value {
    pub fn from_u8(s:&[u8])->Self{
        Value { v: Vec::from(s) }
    }
    pub fn new(s:&str)->Self{
        assert!(s.len() < VALUE_SIZE_LIMIT);
        Value { v: Vec::from(s.as_bytes()) }
    }
    pub fn data(&self)->&[u8]{
        self.v.as_slice()
    }

    pub fn len(&self)->usize{
        self.data().len()
    }
}
