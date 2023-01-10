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
