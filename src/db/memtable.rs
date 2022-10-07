use crate::db::key::Key;
use crate::db::value::Value;

pub struct Memtable {
    // todo
    v:Vec<u8>

}

pub struct MemtableIter<'a> {
    memtable: &'a Memtable,
}

impl Memtable {
    pub fn put(&mut self, key: Key, Value: Value) {
        todo!()
    }
    pub fn get(&mut self, key: Key) -> Option<&Value> {
        todo!()
    }
    pub fn delete(&mut self, key: Key) -> Option<Value> {
        todo!()
    }

    pub fn to_iter(&self) -> MemtableIter {
        todo!()
    }
}

impl <'a>MemtableIter<'a> {


}


impl<'a> Iterator for MemtableIter<'a> {
    type Item = (&'a Key, &'a Value);

    fn next(&mut self) -> Option<Self::Item> {
        let a = &self.memtable.v;
        todo!()
    }
}
