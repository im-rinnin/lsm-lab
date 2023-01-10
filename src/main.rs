#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

use std::fs::File;
use std::rc::Rc;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::thread::{spawn, Thread};
use std::time::{Duration, Instant};

use byteorder::WriteBytesExt;
use log::debug;
use log::warn;

fn work(id: i32) {
    let mut a = 1.1;
    let mut f = File::create(id.to_string()).unwrap();
    loop {
        a = a * 2.2 * 3.2 / 1.2;
        f.write_u8(12).unwrap();
        f.sync_all().unwrap();
    }
}

fn foo(l: Arc<Mutex<i32>>) {
    let mut a = [0; 1000000];
    let mut i = 23;
    for _ in 1..100000 {
        i = (i * 11 + 23) % 1000000;
        a[i] += 3;
    }
}

fn bar() {}

#[derive(Clone)]
struct TestThread {
    a: Arc<Mutex<i32>>,
    b: Arc<Mutex<i32>>,
}

fn main() {
    let a = 2;
    let a = 2;
    let c = 3;
    let d = TestThread {
        a: Arc::new(Mutex::new(3)),
        b: Arc::new(Mutex::new(3)),
    };
    use crossbeam::channel::unbounded;

    // Create a channel that can hold at most 5 messages at a time.
    let (s, r) = unbounded();

    // Can send only 5 messages without blocking.
    for i in 0..5 {
        s.send(i).unwrap();
    }
}
