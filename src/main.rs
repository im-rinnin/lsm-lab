#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::rc::Rc;
use std::sync::atomic::AtomicU64;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::thread::{spawn, Thread};
use std::time::{Duration, Instant};

use byteorder::WriteBytesExt;
use log::debug;
use log::warn;
use metrics::{
    describe_counter, describe_histogram, increment_counter, register_counter, Counter, CounterFn,
    Key,
};
use tempfile::tempfile;

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

struct MetricCounter(Key, AtomicU64);

impl MetricCounter {
    pub fn value(&self) -> u64 {
        self.1.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl CounterFn for MetricCounter {
    fn increment(&self, value: u64) {
        let res = self.1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        println!(
            "counter increment for '{}': {}",
            self.1.load(std::sync::atomic::Ordering::SeqCst),
            value
        );
    }

    fn absolute(&self, value: u64) {
        self.1.store(value, std::sync::atomic::Ordering::SeqCst);
    }
}

#[derive(Default)]
struct MetricRecord {
    counters: Arc<Mutex<HashMap<String, Arc<MetricCounter>>>>,
}
impl MetricRecord {
    pub fn get_counter_value(&self, name: String) -> u64 {
        let map = self.counters.lock().unwrap();
        let res = map.get(&name).unwrap();
        res.value()
    }
}
impl metrics::Recorder for MetricRecord {
    fn describe_counter(
        &self,
        key: metrics::KeyName,
        unit: Option<metrics::Unit>,
        description: metrics::SharedString,
    ) {
    }

    fn describe_gauge(
        &self,
        key: metrics::KeyName,
        unit: Option<metrics::Unit>,
        description: metrics::SharedString,
    ) {
        todo!()
    }

    fn describe_histogram(
        &self,
        key: metrics::KeyName,
        unit: Option<metrics::Unit>,
        description: metrics::SharedString,
    ) {
        todo!()
    }

    fn register_counter(&self, key: &Key) -> metrics::Counter {
        let mut map = self.counters.lock().unwrap();
        if let Some(c) = map.get(key.name()) {
            return Counter::from_arc(c.clone());
        }
        let counter = Arc::new(MetricCounter(key.clone(), AtomicU64::new(0)));
        let res = Counter::from_arc(counter.clone());
        map.insert(String::from(key.name()), counter);
        res
    }

    fn register_gauge(&self, key: &Key) -> metrics::Gauge {
        todo!()
    }

    fn register_histogram(&self, key: &Key) -> metrics::Histogram {
        todo!()
    }
}

struct Timer {
    instant: Instant,
}

impl Timer {
    pub fn new() -> Self {
        Timer {
            instant: Instant::now(),
        }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        println!("{:?}", Instant::now().duration_since(self.instant));
    }
}

fn test_time(i: usize) -> usize {
    let b = Timer::new();
    if i > 10 {
        return 3;
    } else if i > 100 {
        return 4;
    }
    thread::sleep(Duration::from_millis(100));
    let a = i + 4;
    a
}

fn main() {
    let mut file = File::create("test1234").unwrap();

    let a = [1; 1000];

    for i in 0..100 {
        file.write_all(&a).unwrap();
        let now = Instant::now();
        file.sync_all().unwrap();
        println!("{:}", now.elapsed().as_micros());
    }
}
