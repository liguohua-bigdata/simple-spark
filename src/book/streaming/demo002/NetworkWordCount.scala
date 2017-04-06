package streaming.demo002

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by liguohua on 24/02/2017.
  */
object NetworkWordCount {
  def main(args: Array[String]) {
    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    //创建SocketInputDStream，接收来自ip:port发送来的流数据
    val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
