#一、SparkSession简介
```
sparkSession可以视为sqlContext和hiveContext以及StreamingContext的结合体，这些Context的API都可以通过sparkSession使用。  
Spark2.0为了平滑过渡，所以向后兼容，仍然支持hiveContext以及sqlContext。不过官方建议开发者开始使用SparkSession。
```

##1.创建SparkSession
```
要想使用sparksql首先要创建，这是进入sparksql的入口。创建方式有两种，一种是编程创建，另一种是自动创建。编程创建指的是在ide中  
编写代码自己手动创建这个实例，自动创建指的是在spark-shell环境下使用系统为我们创建好的实例。本节主要讲解在spark-shell环境下  
如何使用sparksql.

```
   
###1.1用代码创建SparkSession的实例
```
val spark = SparkSession.builder
    .master("local[2]")
    .appName("spark session example")
    .enableHiveSupport()//使用enableHiveSupport就能够支持hive，相当于hiveContext
    .getOrCreate()
```
###1.2用spark-shell创建好SparkSession  
![](images/Snip20161116_31.png) 
```
1.打开spark-shell
spark-shell --master spark://qingcheng11:7077

2.解释
系统将为我们自动创建好了两个实例，一个是sparkcontext实例'sc'，另一个是sparksession实例'spark'.
```





![](images/Snip20161217_3.png) 