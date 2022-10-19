lsm-db
学习项目

## roadmap 
优先完成最核心的功能, 进行性能测试，分析瓶颈调优，对比leveldb，rocksdb性能，之后再考虑其他功能
核心功能包括
memtable sstable 读，写，sstable compact,元数据（version）读写 

## branch
master 正式代码提交
dev 开发代码
back_for_benchmark_log_metric 功能模块的demo备份

#todo
全局统一配置管理
需要对关键链路进行benchmark，打点, 比如sstable 单文件写，sstable compact

## benchmark

## log
log = "0.4.17"
env_logger = "0.9.0"

### metaric
metrics = "0.20.1"

