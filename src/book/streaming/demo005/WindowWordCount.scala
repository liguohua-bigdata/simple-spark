package streaming.demo005

/**
  * Created by liguohua on 24/02/2017.
  */
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext}

object WindowWordCount {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("WindowWordCount").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // 创建StreamingContext，batch interval为5秒
    val ssc = new StreamingContext(sc, Seconds(5))


    //Socket为数据源
    val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_ONLY_SER)

    val words = lines.flatMap(_.split(" "))

    // windows操作，对窗口中的单词进行计数
    val wordCounts = words.map(x => (x , 1)).reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(10), Seconds(15))

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
