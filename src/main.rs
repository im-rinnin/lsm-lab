use std::cell::RefCell;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::rc::Rc;
use std::thread::spawn;

fn main() {
    debug();
}

trait test {}

fn dynf(a: &mut dyn Iterator<Item=&i32>) {}

struct A {}

impl test for A {}

fn test(a: &mut i32) {
    *a = 5;
    let t = vec![1, 23, 3];
    let mut it = t.iter();

    dynf(&mut it);
}

fn debug() {
    let a = RefCell::new(3);
    let mut d = HashMap::new();
    let s = d.iter();

    for i in s {
        print!("%{}", i.1);
    }
    d.insert(23, 2);

    let h = Rc::new(3);
    h.deref();

    let s = spawn(|| {
        let a = 1;
        return a;
    });
    let res = s.join().unwrap();
    print!("{}", res);
}
