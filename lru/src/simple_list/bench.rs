// 配置项
// 数据大小（固定）
// 初始数据集数量
// 总体数据集数量
// 读写去除比例 通过线程数量实现
// 读miss比例 写使用交替使用两个随机数，一个和读相同，一个和删除相同
struct Config {
    init_size: i32,
    work_load_size: i32,
    date_size: i32,
    read_ratio: i32,
    write_ratio: i32,
    remove_ratio: i32,
    read_miss_ratio: i32,
}
fn bench_test() {
    //     add init date to list
    //     set up read
    //     set up write
    //     set up remove
}

// todo compare vec[],使用全局锁的vec
