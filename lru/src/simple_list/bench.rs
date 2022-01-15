#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
use super::list::List;
use rand::prelude::SliceRandom;
use rand::thread_rng;
use std::panic::take_hook;
use std::sync::Arc;
use std::thread::spawn;

// todo compare vec[],使用全局锁的vec

// 配置项
// 数据大小（固定）
// 初始数据集数量
// 总体数据集数量
// 读,读miss，add,overwrite去除比例
// 并发数
// key生成，使用一个函数将i映射到key空间, i 自增
// 每个线程操作自己写入的数据（读， 去除），key的postfix加上线程特有id将空间分隔
// 使用一个预先shuffer的array作为key生成，每次用一个自增id去取
// key的组成：shuffle的key+线程id
// key,value 大小需要固定，禁止使用string，string会导致内存分配
// 使用一个函数将当前写入数据分割成若干个交替区间，不同的操作（读，override，remove）使用不同的区间，从而保证互相之间不会影响
struct Config {
    work_load_size: i32,
    read_ratio: i32,
    write_ratio: i32,
    remove_ratio: i32,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct Key {
    i: i64,
    miss: i32,
}

impl Key {
    pub fn new(key: i64, miss: i32) -> Key {
        Key { i: key, miss }
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.i != other.i {
            Some(self.i.cmp(&other.i))
        } else {
            Some(self.miss.cmp(&other.miss))
        }
    }
}
#[derive(Debug, Copy, Clone)]
struct Value {
    i: i32,
}

fn build_key(key: i64) -> Key {
    Key::new(key, 0)
}
fn build_value(key: i32) -> Value {
    Value { i: key }
}
pub fn bench_with_default() {
    bench_test_all(4, 10000)
}
pub fn bench_write() {
    fill_data(10000);
}
// fill data
// loop rand operation
pub fn bench_test_all(thread_number: i32, key_space_size: i32) {
    let mut rand_keys = vec![];
    for i in 0..key_space_size {
        rand_keys.push(i as i32);
    }
    let mut rng = thread_rng();
    rand_keys.shuffle(&mut rng);

    let list = fill_data(key_space_size);

    //     set up thread
    let mut joins = vec![];
    for i in 0..thread_number {
        let list_clone = list.clone();
        let mut worker = Worker::new(list_clone, key_space_size as i32, i, rand_keys.clone());
        let join = spawn(move || worker.execute());
        joins.push(join);
    }
    //     wait until finish
    for j in joins {
        j.join().unwrap();
    }
}

fn fill_data(key_space_size: i32) -> Arc<List<Key, Value>> {
    // rand key space
    let list: Arc<List<Key, Value>> = Arc::new(List::new());
    for i in (0..key_space_size).rev() {
        list.add(build_key(i as i64), build_value(i as i32));
    }
    list
}

use crate::rand::simple_rand::Rand;
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fs::read_to_string;
use std::time::Duration;

struct Worker {
    list: Arc<List<Key, Value>>,
    rand_keys: Vec<i32>,
    write_count: i32,
    key_space_size: i32,
    r: Rand,
    thread_id: i32,
    ratio_vec: Vec<i32>,
}
enum Op {
    Read,
    ReadMiss,
    Write,
    OverWrite,
    Remove,
}

impl Worker {
    pub fn new(
        list: Arc<List<Key, Value>>,
        number: i32,
        id: i32,
        rand_keys: Vec<i32>,
    ) -> Box<Self> {
        let read_ratio = 85;
        let write_ratio = 5;
        let read_miss_ratio = 5;
        let overwrite_ratio = 3;
        let remove_ratio = 2;
        let ratios = vec![
            read_ratio,
            read_miss_ratio,
            write_ratio,
            overwrite_ratio,
            read_miss_ratio,
        ];
        Box::new(Worker {
            list,
            rand_keys,
            write_count: 0,
            r: Rand::with_seed(id as u64),
            thread_id: id,
            ratio_vec: ratios,
            key_space_size: number,
        })
    }

    pub fn execute(&mut self) {
        loop {
            let op = self.get_op();
            match op {
                Op::Write => self.write(),
                Op::OverWrite => self.read_miss(),
                Op::Remove => self.remove(),
                Op::Read => self.read(),
                Op::ReadMiss => {}
            }
        }
    }

    fn get_op(&mut self) -> Op {
        let mut n = (self.r.next() as i32) % 100;
        n -= *(self.ratio_vec.get(0).unwrap());
        if n < 0 {
            return Op::Read;
        }
        n -= *(self.ratio_vec.get(1).unwrap());
        if n < 0 {
            return Op::ReadMiss;
        }
        n -= *(self.ratio_vec.get(2).unwrap());

        if n < 0 {
            return Op::Write;
        }
        n -= *(self.ratio_vec.get(3).unwrap());

        if n < 0 {
            return Op::OverWrite;
        }
        return Op::Remove;
    }

    fn read(&mut self) {
        //
        let key = self.random_key_already_write();
        let list = self.list.borrow() as &List<Key, Value>;
        let res = list.get(key);
        assert!(res.is_some());
    }
    fn read_miss(&self) {
        //     todo
    }
    fn write(&mut self) {
        // get rand key from shuffle array
        let i = self.write_count as usize;
        let key = self.rand_keys.get(i).unwrap();
        self.write_count = (self.write_count + 1) % self.key_space_size;

        let list_key = build_key(*key as i64);
        let list_value = build_value(*key);
        (self.list.borrow() as &List<Key, Value>).add(list_key, list_value);
    }
    fn overwrite(&self) {
        // todo
        unimplemented!()
    }
    fn remove(&self) {
        //     todo
    }

    fn random_key_already_write(&mut self) -> Key {
        let key = self
            .rand_keys
            .get((self.r.next() as usize) % (self.key_space_size as usize))
            .unwrap();
        build_key(*key as i64)
    }
}

#[cfg(test)]
mod test {
    use crate::simple_list::bench::{bench_test_all, bench_with_default, Key};
    // #[test]
    // fn test_bench() {
    //     bench_test_all(2, 1000);
    // }
    //
    // #[test]
    // fn test_bench_write() {
    //     bench_test_all(2, 1000);
    // }
    #[test]
    fn test() {
        let a = Key { i: 2, miss: 3 };
        let b = Key { i: 2, miss: 1 };

        print!("{}", a > b);
    }
}
