#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub sstable_file_limit: usize,
    pub level_0_file_limit: usize,
    pub level_size_expand_factor: usize,
}

impl Config {
    pub fn new() -> Self {
        Config {
            sstable_file_limit: 2 * 1024 * 1024,
            level_0_file_limit: 4,
            level_size_expand_factor: 10,
        }
    }
}
