use std::time::Duration;

#[derive(Clone, Debug)]
pub struct Config {
    pub sstable_file_limit: usize,
    pub level_0_file_limit: usize,
    pub level_size_expand_factor: usize,
    pub meta_log_file_name: String,
    pub sstable_meta_cache: usize,
    pub memtable_size_limit: usize,
    pub level_0_len_to_slow_write_threshold: usize,
    pub memtable_log_file_path: String,
    pub request_write_batch_size: usize,
    pub request_write_buffer_wait_time: Duration,
}

impl Config {
    pub fn new() -> Self {
        Config {
            sstable_file_limit: 2 * 1024 * 1024,
            level_0_file_limit: 4,
            level_size_expand_factor: 10,
            meta_log_file_name: String::from("meta"),
            sstable_meta_cache: 100,
            memtable_size_limit: 2 * 1024 * 1024,
            level_0_len_to_slow_write_threshold: 4,
            memtable_log_file_path: String::from("memtable_log"),
            request_write_batch_size: 1 << 20,
            request_write_buffer_wait_time: Duration::from_micros(25),
        }
    }
}
