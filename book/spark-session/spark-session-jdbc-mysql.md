#一、SparkSession读取Mysql数据库的四种方式讲解


#二、SparkSession常用API
##1.不指定查询条件
方法原型
```
def jdbc(url: String, table: String, properties: Properties): DataFrame
spark默认开启一个线程拉取所有的数据，
一方面，并行度太低，效率太低。
另一方面，如果MySQL表中的数据较大，很有可能出现OOM.
```
执行程序
```scala

```
执行效果


##1.不指定查询条件
```

```
执行程序
```scala

```
执行效果




##1.不指定查询条件
```

```
执行程序
```scala

```
执行效果




##1.不指定查询条件
```

```
执行程序
```scala

```
执行效果







