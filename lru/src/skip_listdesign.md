## 线程安全

操作，add（包括overwrite），get，delete

## 实现

每一层是一个线程安全的list 上层维护底层的raw point（并不是owner） level 0：(key,value)
level n(n>1):(key,point to level node)

index node(level node > 0) 不会也不能出现重复key base
node可以出现重复key，因为delete只会给node打上delete标签，后续insert会在delete node后加入

## list 需要增加的功能

- [ ] cas insert

- [x] 从某个节点开始寻找,找到最后一个小于或者等于的节点

- [x] gc去掉，gc由skiplist完成

- [x] add 返回增加（也可能是overwrite）节点的raw point

## question

如何删除 只是对 base list，进行删除mark，上层节点不需要处理，后续由gc完成回收, 因为list删除后增加的node会放在最后一个delete
node，所以结构是这样的 某个key第一次增加到list

```
               ┌──────────┐
               │          │
               │  index   │
               │          │
               │          │
               └─────┬────┘
                     │
                     │
                     │
                     │
               ┌─────▼────┐
               │          │
               │          │
       base    │   alived │
               │          │
               └──────────┘
```

多次删除增加同一个key

                  ┌───────┐
                  │ index │
                  │       │
                  └───┬───┘
                      │
                  ┌───▼───┐  ┌─────────┐   ┌──────────┐
                  │deleted│  │ deleted │   │          │
       base_level │       ├──►         ├───┤►alived   │
                  └───────┘  └─────────┘   └──────────┘

如何gc 和list一样用一个全局锁锁住整个skip list 从最高层开始，发现某个节点删除后，把该节点从该层中删除,第n层遍历结束对n-1层重复上述过程

如何增加level add增加了节点后，list返回新增节点的指针，从低层到高层逐个增加新节点

何时gc remove计数达到阈值新启动
