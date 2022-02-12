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
