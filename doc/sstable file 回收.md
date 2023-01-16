# sstable file 回收

## 如何知道哪些需要回收

version 包含所有sstable文件，当version不再使用，可以进行回收

## 实现

### 关键结构

清理线程，负责维护引用计数结构和文件删除
引用计数，file_id->counter
sstable增减channel，通过这个channel向清理线程发生sstable 文件引用 释放/增加的消息

### 初始化

初始化需要得到目前所有正在使用的sstable file，并删除没有用的
需要一个引用计数结构 file_id->use counter
这里需要借助version的信息，因为所有 sstable 被某个/多个 version own

### 回收

由version 的drop触发，这里为了把删除流程和version 解耦（同时也是方便测试），使用一个channel传递这个version所使用的file ids

channel 接受者，即清理线程，会对引用计数进行修正,并释放不需要的sstable

## 增加sstable

在新的version创建出来后，需要将其加入引用计数，这里同样通过channel的方法将其传递给清理线程，主要是同步channel
