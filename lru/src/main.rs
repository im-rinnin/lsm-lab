extern crate lru;
use lru::simple_list::bench::bench_with_default;
use std::borrow::{Borrow, BorrowMut};
use std::fs::File;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::thread::spawn;
use std::time::Duration;

fn main() {
    println!("start");
    bench_with_default();
}

// fn main() {
//     let mut j = None;
//     let lock = Arc::new(Mutex::new(1));
//     for i in 0..1 {
//         let lock_clone = lock.clone();
//         j = Some(spawn(move || {
//             let mut a = 3;
//             loop {
//                 a += 4;
//                 // if a > 23432 {
//                 a /= 2;
//                 let mut h: &Mutex<i32> = lock_clone.borrow();
//                 let mut s = h.lock().unwrap();
//                 (*s) += a;
//                 // }
//             }
//         }));
//     }
//     j.unwrap().join();
// }
