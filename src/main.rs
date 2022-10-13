use std::cell::RefCell;
use std::collections::HashMap;
use std::env::ArgsOs;
use std::fs::File;
use std::io::{BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::ops::{Deref, DerefMut};
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::spawn;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use metrics::{Counter, CounterFn, Gauge, GaugeFn, Histogram, HistogramFn, Key, KeyName, Recorder, SharedString, Unit};

#[derive(Default)]
struct PrintRecorder;

struct PrintHandle(Key, AtomicU64);


// impl PrintHandle {
//     pub fn new(key:Key)->Self{
//         PrintHandle(key,AtomicU64::new(0))
//     }
// }


impl CounterFn for PrintHandle {
    fn increment(&self, value: u64) {
        CounterFn::increment(&self.1, value);
        println!("counter increment for '{}': {}", self.0, self.1.load(Ordering::Relaxed));
    }

    fn absolute(&self, value: u64) {
        println!("counter absolute for '{}': {}", self.0, value);
    }
}

impl GaugeFn for PrintHandle {
    fn increment(&self, value: f64) {
        println!("gauge increment for '{}': {}", self.0, value);
    }

    fn decrement(&self, value: f64) {
        println!("gauge decrement for '{}': {}", self.0, value);
    }

    fn set(&self, value: f64) {
        println!("gauge set for '{}': {}", self.0, value);
    }
}

impl HistogramFn for PrintHandle {
    fn record(&self, value: f64) {
        println!("histogram record for '{}': {}", self.0, value);
    }
}

impl Recorder for PrintRecorder {
    fn describe_counter(&self, key_name: KeyName, unit: Option<Unit>, description: SharedString) {
        println!(
            "(counter) registered key {} with unit {:?} and description {:?}",
            key_name.as_str(),
            unit,
            description
        );
    }

    fn describe_gauge(&self, key_name: KeyName, unit: Option<Unit>, description: SharedString) {
        println!(
            "(gauge) registered key {} with unit {:?} and description {:?}",
            key_name.as_str(),
            unit,
            description
        );
    }

    fn describe_histogram(&self, key_name: KeyName, unit: Option<Unit>, description: SharedString) {
        println!(
            "(histogram) registered key {} with unit {:?} and description {:?}",
            key_name.as_str(),
            unit,
            description
        );
    }

    fn register_counter(&self, key: &Key) -> Counter {
        Counter::from_arc(Arc::new(PrintHandle(key.clone(), AtomicU64::new(0))))
    }

    fn register_gauge(&self, key: &Key) -> Gauge {
        Gauge::from_arc(Arc::new(PrintHandle(key.clone(), AtomicU64::new(0))))
    }

    fn register_histogram(&self, key: &Key) -> Histogram {
        Histogram::from_arc(Arc::new(PrintHandle(key.clone(), AtomicU64::new(0))))
    }
}

fn init_print_logger() {
    let recorder = PrintRecorder::default();
    metrics::set_boxed_recorder(Box::new(recorder)).unwrap()
}
struct foo{
    a:u32
}

impl foo {
    fn aa<'a>(&'a self)->&'a u32{
        &self.a
    }
}


fn main() {
    let v = vec![1, 2, 3, 4, 9];
    let mut tmpfile: File = tempfile::tempfile().unwrap();
    tmpfile.write_u32::<LittleEndian>(32);
    tmpfile.metadata().unwrap().len();
    println!("{:}",tmpfile.read_u32::<LittleEndian>().unwrap());

    println!("{:}", v.partition_point(|n|*n<3));
    // let a = 3;
    // init_print_logger();
    // let c = metrics::register_counter!("abc");
    // c.increment(1);
    // c.increment(1);
    //
    // debug().unwrap();
    //  use dashmap::DashMap;
    //
    //  let youtubers = DashMap::new();
    //  youtubers.insert("Bosnian Bill", 457000);
    //  assert_eq!(*youtubers.get("Bosnian Bill").unwrap(), 457000);
    // let h=youtubers.get("Bosnian Bill").unwrap();
    // let s=*h;
    // thread()
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

#[derive(Default)]
struct t {
    a: Mutex<u32>,
    b: u32,
}

fn goo(map: &mut HashMap<i32, i32>) -> Option<&i32> {
    map.insert(1, 1);
    map.get(&2)
}

fn thread() {
    use std::thread;
    let mut a = dashmap::DashMap::new();
    a.insert(1, 1);
    let c = a.iter();


    let counter = Arc::new(t::default());
    let mut handles = vec![];

    for _ in 0..10 {
        let counter = Arc::clone(&counter);
        let map = a.clone();
        let handle = thread::spawn(move || {
            map.insert(2, 3);
            let mut num = counter.a.lock().unwrap();
            *num += 1;
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

fn hoo()->usize{
    let a = 1;
    a
}


fn debug() -> anyhow::Result<()> {
    use byteorder::{LittleEndian, WriteBytesExt};
    let s = vec![1, 2, 3];
    let ss = vec![12];
    let mut b = [1; 1];
    let mut d = b.as_mut_slice();
    // d.write_u32::<LittleEndian>(34)?;
    let mut t = BufWriter::with_capacity(10, s);
    t.write(b.as_slice());
    t.flush()?;


    let a = 32;
    let mut b: Vec<u8> = Vec::with_capacity(100);
    let c = Cursor::new(b);
    // let buf = BufReader::new(b.as_slice());
    // Box::new(buf) as dyn Read;
    // b.write_u16::<LittleEndian>(1024).unwrap();
    Ok(())
}