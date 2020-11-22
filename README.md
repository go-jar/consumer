# 思路

- consumer：从消息队列中获取任务。
- dispatcher：决定由哪个 worker 执行任务。
- worker：执行任务。

# 参考

- https://www.jianshu.com/p/593cc8647ad2
- https://github.com/goinbox/goconsumer
