use std::fs::File;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

use byteorder::WriteBytesExt;

fn work(id: i32) {
    let mut a = 1.1;
    let mut f = File::create(id.to_string()).unwrap();
    loop {
        a = a * 2.2 * 3.2 / 1.2;
        f.write_u8(12);
        f.sync_all();
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

fn main() {
    let data = Arc::new(Mutex::new(0));
    for i in 1..2 {
        let clone = data.clone();
        thread::spawn(move || {
            foo(clone);
        });
    }
    foo(data);
    // std::thread::sleep(std::time::Duration::from_secs(10000));
}
