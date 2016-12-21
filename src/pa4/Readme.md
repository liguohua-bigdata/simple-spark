##二、spark的特性
###1.执行速度快
![](images/logistic-regression.png) 
```
1.spark是基于内存的分布式计算框架，它比基于磁盘的MapReduce快的多。
2.spark在job执行时会根据任务关系的DAG图进行计算优化。
```
###2.软件栈丰富
![](images/spark-stack.png) 
```
1.spark的软件栈比较丰富，这些软件栈能够处理大量的业务场景。
2.spark支持批处理，流处理，sql,图计算，机器学习等。
```

###3.spark架构图
![](images/cluster-overview.png) 
```
1.主节点master负责分发任务，并监控从节点上的任务执行情况。
2.从节点worker的负责执行任务，并报任务进度给主节点。
```

###4.spark内部工作原理
![](images/sparkapp-sparkcontext-master-slaves.png) 
```
1.spark集群可以包含多个executor.
2.每个executor中可以并行执行多个task
```
###5.RDD的概念
![](images/spark-rdd-partitioned-distributed.png) 
```
1.spark中的基本概念是Resilient Distributed Dataset (RDD)
2.rdd中的数据被分散到集群的各个机器上以便进行并行运算。
```
###6.RDD的特性
```
In-Memory, i.e. data inside RDD is stored in memory as much (size) and long (time) as possible.

Immutable or Read-Only, i.e. it does not change once created and can only be transformed using transformations to new RDDs.

Lazy evaluated, i.e. the data inside RDD is not available or transformed until an action is executed that triggers the execution.

Cacheable, i.e. you can hold all the data in a persistent "storage" like memory (default and the most preferred) or disk (the least preferred due to access speed).

Parallel, i.e. process data in parallel.

Typed, i.e. values in a RDD have types, e.g. RDD[Long] or RDD[(Int, String)].

Partitioned, i.e. the data inside a RDD is partitioned (split into partitions) and then distributed across nodes in a cluster (one partition per JVM that may or may not correspond to a single node).
```



参考链接：
http://blog.csdn.net/shifenglov/article/details/43795597
http://blog.csdn.net/qq_19341327/article/details/50815356
https://segmentfault.com/a/1190000005034280
http://spark.apache.org/docs/latest/mllib-collaborative-filtering.html
https://www.zhihu.com/question/31509438
http://blog.csdn.net/oopsoom/article/details/34462329