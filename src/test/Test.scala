package test

/**
  * Created by zwx on 5/1/15.
  */
import org.apache.spark._

object Test {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("remote")
      .setMaster("local[*]")

    val spark = new SparkContext(conf)
    val rdd = spark.textFile("alluxio://qingcheng11:19998/input/flink/README.txt")
    rdd.collect().foreach(print(_))
    spark.stop()
  }
}