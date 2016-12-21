![](images/logistic-regression.png) 
```
1.spark是基于内存的分布式计算框架，它比基于磁盘的MapReduce快的多。
2.spark在job执行时会根据任务关系的DAG图进行计算优化。
```

![](images/spark-stack.png) 
```
1.spark的软件栈比较丰富，这些软件栈能够处理大量的业务场景。
2.spark支持批处理，流处理，sql,图计算，机器学习等。
```


![](images/cluster-overview.png) 
```
1.主节点master负责分发任务，并监控从节点上的任务执行情况。
2.从节点worker的负责执行任务，并报任务进度给主节点。
```


![](images/sparkapp-sparkcontext-master-slaves.png) 
```
1.spark集群可以包含多个executor.
2.每个executor中可以并行执行多个task
```

![](images/spark-rdd-partitioned-distributed.png) 
```
1.spark中的基本概念是rdd
2.rdd中的数据被分散到集群的各个机器上以便进行并行运算。
```



参考链接：
http://blog.csdn.net/shifenglov/article/details/43795597
http://blog.csdn.net/qq_19341327/article/details/50815356
https://segmentfault.com/a/1190000005034280
http://spark.apache.org/docs/latest/mllib-collaborative-filtering.html
https://www.zhihu.com/question/31509438
http://blog.csdn.net/oopsoom/article/details/34462329