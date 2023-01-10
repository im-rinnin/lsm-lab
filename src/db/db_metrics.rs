use std::{
    collections::HashMap,
    fmt::Display,
    sync::{
        atomic::{self, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Instant,
};

use histogram::Histogram as HistogramImpl;
use log::{debug, info};
use metrics::{
    absolute_counter, gauge, histogram, increment_counter, Counter, CounterFn, Gauge, GaugeFn,
    Histogram, HistogramFn, Key,
};
//  metric names
pub const FILE_NUMBER_METRIC_PREFIX: &str = "file_number.level.";
pub const CURRENT_LEVEL_DEPTH: &str = "current_level";
pub const WRITE_REQUEST_COUNT: &str = "write_request.count";
pub const READ_REQUEST_COUNT: &str = "read_request.count";
pub const WRITE_REQUEST_TIME: &str = "write_request.time";
pub const READ_REQUEST_TIME: &str = "read_request.time";
pub const COMPACT_COUNT: &str = "compact.count";
pub const SSTABLE_COMPACT_TIME: &str = "sstable_compatct.time";

pub const READ_HIT_MEMTABLE_COUNTER: &str = "read_request.hit_memtable";
pub const READ_HIT_SSTABLE_LEVEL: &str = "read_request.hit_sstable_level";

pub const WRITE_WAIT_FOR_COMAPCT: &str = "write_request.wait_for_comapct";

/////////////////////////////
///

pub fn init_metric() -> MetricExporter {
    let (record, exporter) = MetricRecord::new();
    metrics::set_boxed_recorder(Box::new(record)).unwrap();
    exporter
}

pub struct HistogramMetric {
    name: String,
    histogram: Arc<Mutex<HistogramImpl>>,
}

impl HistogramMetric {
    pub fn new(name: &str) -> Self {
        HistogramMetric {
            name: name.to_string(),
            histogram: Arc::new(Mutex::new(HistogramImpl::new())),
        }
    }
}

impl HistogramFn for HistogramMetric {
    fn record(&self, value: f64) {
        let mut lock = self.histogram.lock().unwrap();
        lock.increment(value as u64).unwrap();
    }
}
impl Display for HistogramMetric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let histogram = self.histogram.lock().unwrap();
        write!(
            f,
            "histogram name: {:}, avg: {:}, min {:}, max {:} ,10_p {:}, 50_p {:},90_percent: {:}",
            self.name,
            histogram.mean().unwrap(),
            histogram.minimum().unwrap(),
            histogram.maximum().unwrap(),
            histogram.percentile(10.0).unwrap(),
            histogram.percentile(50.0).unwrap(),
            histogram.percentile(90.0).unwrap(),
        )?;
        Ok(())
    }
}

pub struct TimeRecorder {
    start_time: Instant,
    name: String,
}

impl TimeRecorder {
    pub fn new(name: &str) -> Self {
        TimeRecorder {
            start_time: Instant::now(),
            name: name.to_string(),
        }
    }
}

impl Drop for TimeRecorder {
    fn drop(&mut self) {
        let time = self.start_time.elapsed().as_micros() as f64;
        histogram!(self.name.clone(), time);
    }
}

struct CounterMetric(Key, AtomicU64);

impl CounterMetric {
    pub fn value(&self) -> u64 {
        self.1.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl CounterFn for CounterMetric {
    fn increment(&self, value: u64) {
        self.1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn absolute(&self, value: u64) {
        self.1.store(value, std::sync::atomic::Ordering::SeqCst);
    }
}
struct GaugeMetric(Key, AtomicU64);

impl GaugeMetric {
    pub fn value(&self) -> u64 {
        self.1.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl GaugeFn for GaugeMetric {
    fn increment(&self, value: f64) {
        self.1.fetch_add(value as u64, Ordering::SeqCst);
    }

    fn decrement(&self, value: f64) {
        self.1.fetch_sub(value as u64, Ordering::SeqCst);
    }

    fn set(&self, value: f64) {
        self.1.store(value as u64, Ordering::SeqCst);
    }
}
pub struct MetricExporter {
    counters: Arc<Mutex<HashMap<String, Arc<CounterMetric>>>>,
    gauges: Arc<Mutex<HashMap<String, Arc<GaugeMetric>>>>,
    histograms: Arc<Mutex<HashMap<String, Arc<HistogramMetric>>>>,
}

impl Drop for MetricExporter {
    fn drop(&mut self) {
        self.log_current_metric();
    }
}

impl MetricExporter {
    pub fn get_counter_value(&self, name: &str) -> u64 {
        let map = self.counters.lock().unwrap();
        let counter = map.get(name).unwrap();

        let res = counter.1.load(Ordering::SeqCst);
        res
    }
    pub fn get_gauge_value(&self, name: &str) -> u64 {
        let map = self.gauges.lock().unwrap();
        let c = map.get(name).unwrap();

        let res = c.1.load(Ordering::SeqCst);
        res
    }

    pub fn log_current_metric(&self) {
        info!("log metric start");
        let map = self.counters.lock().unwrap();
        for (name, counter) in map.iter() {
            info!(
                "metric: {:} counte value is {:}",
                name,
                counter.1.load(Ordering::SeqCst)
            );
        }
        let map = self.gauges.lock().unwrap();
        for (name, gauge) in map.iter() {
            info!(
                "metric: {:} gauge value is {:}",
                name,
                gauge.1.load(Ordering::SeqCst)
            );
        }
        let map = self.histograms.lock().unwrap();
        for (name, histogram) in map.iter() {
            info!(
                "metric: {:} histogram value is {:}",
                name,
                histogram.to_string()
            );
        }
        info!("log metric end");
    }
}

#[derive(Default)]
struct MetricRecord {
    counters: Arc<Mutex<HashMap<String, Arc<CounterMetric>>>>,
    gauges: Arc<Mutex<HashMap<String, Arc<GaugeMetric>>>>,
    histograms: Arc<Mutex<HashMap<String, Arc<HistogramMetric>>>>,
}
impl MetricRecord {
    pub fn new() -> (Self, MetricExporter) {
        let recorder = Self::default();
        let exporter = MetricExporter {
            counters: recorder.counters.clone(),
            gauges: recorder.gauges.clone(),
            histograms: recorder.histograms.clone(),
        };
        (recorder, exporter)
    }
}
impl metrics::Recorder for MetricRecord {
    fn describe_counter(
        &self,
        key: metrics::KeyName,
        unit: Option<metrics::Unit>,
        description: metrics::SharedString,
    ) {
        todo!()
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
        let counter = Arc::new(CounterMetric(key.clone(), AtomicU64::new(0)));
        let res = Counter::from_arc(counter.clone());
        map.insert(String::from(key.name()), counter);
        res
    }

    fn register_gauge(&self, key: &Key) -> metrics::Gauge {
        let mut map = self.gauges.lock().unwrap();
        if let Some(g) = map.get(key.name()) {
            return Gauge::from_arc(g.clone());
        }
        let g = Arc::new(GaugeMetric(key.clone(), AtomicU64::new(0)));
        let res = Gauge::from_arc(g.clone());
        map.insert(String::from(key.name()), g);
        res
    }

    fn register_histogram(&self, key: &Key) -> metrics::Histogram {
        let mut map = self.histograms.lock().unwrap();
        if let Some(h) = map.get(key.name()) {
            return Histogram::from_arc(h.clone());
        }
        let h = Arc::new(HistogramMetric::new(key.name()));
        let res = Histogram::from_arc(h.clone());
        map.insert(String::from(key.name()), h);
        res
    }
}

#[derive(Debug)]
pub struct DBMetric {
    level_0_file_number: Vec<AtomicU64>,
}

impl DBMetric {
    pub fn new() -> Self {
        DBMetric {
            level_0_file_number: vec![
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
            ],
        }
    }

    pub fn set_level_n_file_number(&self, size: u64, level: usize) {
        debug!("set_level {:} file_number to {}", level, size);

        let metric_name = FILE_NUMBER_METRIC_PREFIX.to_string() + (&level.to_string());
        absolute_counter!(metric_name, size);

        self.level_0_file_number
            .get(level)
            .unwrap()
            .store(size, Ordering::SeqCst);
    }

    pub fn get_level_n_file_number(&self, level: usize) -> u64 {
        let res = self
            .level_0_file_number
            .get(level)
            .unwrap()
            .load(Ordering::SeqCst);
        res
    }
}

#[cfg(test)]
mod test {
    use std::{thread, time::Duration};

    use metrics::{gauge, histogram, increment_counter, increment_gauge};

    use crate::db::db_metrics::TimeRecorder;

    use super::MetricRecord;

    // can't registe metric more than once
    // #[test]
    fn test_metric() {
        let (record, exporter) = MetricRecord::new();
        metrics::set_boxed_recorder(Box::new(record)).unwrap();
        increment_counter!("metric.test");
        increment_counter!("metric.test");
        increment_counter!("metric.test");
        gauge!("metric.test_gauge", 10.0);

        assert_eq!(exporter.get_counter_value("metric.test"), 3);
        assert_eq!(exporter.get_gauge_value("metric.test_gauge"), 10);

        histogram!("metric.histogram", 10.0);
        histogram!("metric.histogram", 20.0);
        histogram!("metric.histogram", 10.0);

        {
            let c = TimeRecorder::new("test_time");
            thread::sleep(Duration::from_millis(2));
        }
        exporter.log_current_metric();
    }
}
