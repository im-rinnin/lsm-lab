#[derive(Clone)]
pub struct Value {
    v: Vec<u8>,
}

impl Value {
    pub fn new(s:&String)->Self{
        Value { v: Vec::from(s.as_bytes()) }
    }
    pub fn data(&self)->&[u8]{
        self.v.as_slice()
    }
}
