一、在spark-shell中使用SparkContext
##1.查看sparkContext
```
1.启动spark-shell查看sparkContext
$SPARK_HOME/bin/spark-shell --master spark://qingcheng11:7077
```
![](images/Snip20161216_2.png) 

##2.sparkContext中可用方法概览
```
输入sc,并按tab键，自动提示sparkcontext下的可用方法
```
![](images/Snip20161216_3.png) 
二、SparkContext常用API实战
##1.version
```
用于显示spark的版本信息。
```
执行命令
```
sc.version
```
执行效果
```
res1: String = 2.0.1
```
##2.getConf
```
用于获取spark的配置信息。
```
执行命令
```
sc.getConf
```
执行效果
```
res2: org.apache.spark.SparkConf = org.apache.spark.SparkConf@4d9cf71d
```

##3.getExecutorMemoryStatus
```
获取executor上的内存使用情况。
```
执行命令
```
sc.getExecutorMemoryStatus
```
执行效果
```
res3: scala.collection.Map[String,(Long, Long)] =Map(
192.168.0.12:44073 -> (434031820,434031820),
192.168.0.11:49206 -> (434031820,434031820),
192.168.0.11:57599 -> (434031820,434031820), 
192.168.0.13:49891 -> (434031820,434031820))
```

##4.master
```
获取当前master信息。
```
执行命令
```
sc.master
```
执行效果
```
res4: String = spark://qingcheng11:7077
```

##5.appName
```
获取当前application名称。
```
执行命令
```
sc.appName
```
执行效果
```
res6: String = Spark shell
```

##6. deployMode
```
查看当前spark的部署模式
```
执行命令
```
 sc.deployMode
```
执行效果
```
res7: String = client
```

##7.getExecutorStorageStatus
```
获取executor上的磁盘使用情况。
```
执行命令
```
sc.getExecutorStorageStatus.collect
```
执行效果
```
res11: Array[org.apache.spark.storage.StorageStatus] = Array(
org.apache.spark.storage.StorageStatus@2e5ff1d4, 
org.apache.spark.storage.StorageStatus@1a16583d, 
org.apache.spark.storage.StorageStatus@59da4992, 
org.apache.spark.storage.StorageStatus@20425fab)
```

##8.uiWebUrl
```
获取webUI的url
```
执行命令
```
sc.uiWebUrl
```
执行效果
```
res15: Option[String] = Some(http://192.168.0.11:4040)
```


##9.getPersistentRDDs

```
获取缓存的rdd
```
执行命令
```
//1.创建rdd
val rdd1=sc.parallelize(Seq("a","b","c"))

//2.缓存rdd
rdd1.cache

//3.显示缓存的rdd
sc.getPersistentRDDs
```
执行效果
```
scala> sc.getPersistentRDDs
res24: scala.collection.Map[Int,org.apache.spark.rdd.RDD[_]] = Map(0 -> ParallelCollectionRDD[0] at parallelize at <console>:24)
```


##0.
```
```
执行命令
```
```
执行效果
```
```


##0.
```
```
执行命令
```
```
执行效果
```
```


##0.
```
```
执行命令
```
```
执行效果
```
```


##0.
```
```
执行命令
```
```
执行效果
```
```


##0.
```
```
执行命令
```
```
执行效果
```
```


##0.
```
```
执行命令
```
```
执行效果
```
```


##0.
```
```
执行命令
```
```
执行效果
```
```


##0.
```
```
执行命令
```
```
执行效果
```
```


##0.
```
```
执行命令
```
```
执行效果
```
```



