package book.sparksql.sparksession

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by liguohua on 07/12/2016.
  */
object RDDToDataSet {

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .enableHiveSupport()
      .appName("RDDToDataSet")
      .getOrCreate()
    import sparkSession.implicits._

    val sparkContext = sparkSession.sparkContext


    //read data from text file
    val filePath="hdfs://qingcheng12:9000/input/spark/README.md"
    val rdd = sparkContext.textFile(filePath)
    val dataset = sparkSession.read.text(filePath).as[String]

    //do show
    rdd.foreach(println(_))
    dataset.show()

    // do count
    println("count ")
    println(rdd.count())
    println(dataset.count())

    // wordcount
    println(" wordcount ")

    val wordsRDD = rdd.flatMap(value => value.split("\\s+"))
    val wordsPair = wordsRDD.map(word => (word, 1))
    val wordCount = wordsPair.reduceByKey(_ + _)
    println(wordCount.collect.toList)

    val wordsDs = dataset.flatMap(value => value.split("\\s+"))
    val wordsPairDs = wordsDs.groupByKey(value => value)
    val wordCountDs = wordsPairDs.count
    wordCountDs.show()

    //cache
    rdd.cache()
    dataset.cache()

    //filter
    val filteredRDD = wordsRDD.filter(value => value == "hello")
    println(filteredRDD.collect().toList)

    val filteredDS = wordsDs.filter(value => value == "hello")
    filteredDS.show()


    //map partitions

    val mapPartitionsRDD = rdd.mapPartitions(iterator => List(iterator.count(value => true)).iterator)
    println(s" the count each partition is ${mapPartitionsRDD.collect().toList}")

    val mapPartitionsDs = dataset.mapPartitions(iterator => List(iterator.count(value => true)).iterator)
    mapPartitionsDs.show()

    //converting to each other
    val dsToRDD = dataset.rdd
    println(dsToRDD.collect())

    val rddStringToRowRDD = rdd.map(value => Row(value))
    val dfschema = StructType(Array(StructField("value", StringType)))
    val rddToDF = sparkSession.createDataFrame(rddStringToRowRDD, dfschema)
    val rDDToDataSet = rddToDF.as[String]
    rDDToDataSet.show()

    // double based operation

    val doubleRDD = sparkContext.makeRDD(List(1.0, 5.0, 8.9, 9.0))
    val rddSum = doubleRDD.sum()
    val rddMean = doubleRDD.mean()

    println(s"sum is $rddSum")
    println(s"mean is $rddMean")

    val rowRDD = doubleRDD.map(value => Row.fromSeq(List(value)))
    val schema = StructType(Array(StructField("value", DoubleType)))
    val doubleDS = sparkSession.createDataFrame(rowRDD, schema)

    import org.apache.spark.sql.functions._
    doubleDS.agg(sum("value")).show()
    doubleDS.agg(mean("value")).show()

    //reduceByKey API
    val reduceCountByRDD = wordsPair.reduceByKey(_ + _)
    val reduceCountByDs = wordsPairDs.mapGroups((key, values) => (key, values.length))

    println(reduceCountByRDD.collect().toList)
    println(reduceCountByDs.collect().toList)

    //reduce function
    val rddReduce = doubleRDD.reduce((a, b) => a + b)
    val dsReduce = doubleDS.reduce((row1, row2) => Row(row1.getDouble(0) + row2.getDouble(0)))

    println("rdd reduce is " + rddReduce + " dataset reduce " + dsReduce)

  }

}
