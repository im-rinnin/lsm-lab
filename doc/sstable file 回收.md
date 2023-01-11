# sstable file 回收

## 如何知道哪些需要回收

version 包含所有sstable文件，当version不再使用，可以进行回收

## 实现

使用sstable_collector类进行回收管理，包含所有的sstable file和引用计数
在打开数据库时进行初始化
当version drop时，将自己所有的file name 传给sstable collector，由其对计数进行统计，并完成文件删除
sstable collector由version share own(```arc<mutex>```)

另外，在open db时，在version建立后，需要进行一次 sstable 清理,处理上次关闭数据库没有正常清理掉的 sstable 文件
