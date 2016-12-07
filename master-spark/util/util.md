##一、在spark-standalone模式下查看master状态
###1.jps方法
```scala
jps -lm
```
![](images/Snip20161130_2.png) 
###2.spark-daemon.sh方法
```scala
$SPARK_HOME/sbin/spark-daemon.sh status org.apache.spark.deploy.master.Master 1
```
![](images/Snip20161130_3.png) 


