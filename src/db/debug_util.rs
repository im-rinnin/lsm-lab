use std::{
    sync::mpsc::{sync_channel, SyncSender},
    thread::spawn,
};

use crossbeam::channel::{unbounded, Receiver, Sender};
use simplelog::*;

use super::db_metrics::{init_metric, MetricExporter};
pub fn init_test_log_as_info_and_metric() -> MetricExporter {
    SimpleLogger::init(LevelFilter::Info, Config::default()).unwrap();
    // initialization code here
    init_metric()
}
pub fn init_test_log_as_debug_and_metric() -> MetricExporter {
    SimpleLogger::init(LevelFilter::Debug, Config::default()).unwrap();
    // initialization code here
    init_metric()
}

pub fn dump_recv<T: Send + 'static>(r: Receiver<T>) {
    spawn(move || loop {
        let a = r.recv();
        if a.is_err() {
            break;
        }
    });
}

#[test]
pub fn test() {
    let a = "sddf";
    let (s, r) = unbounded();
    dump_recv(r);
    s.send(1).unwrap();
    s.send(2).unwrap();
    s.send(2).unwrap();
}
