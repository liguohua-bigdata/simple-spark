package test
/**
  * Created by zwx on 5/1/15.
  */
import org.apache.spark._

object RemoteRunTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("remote")
      .setMaster("spark://qingcheng11:7077")

    val spark = new SparkContext(conf)
    val rdd = spark.textFile("hdfs://qingcheng11:9000/input/flink/README.txt")
    rdd.collect().foreach(print(_))
    spark.stop()
  }
}