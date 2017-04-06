package streaming.demo006

/**
  * Created by liguohua on 24/02/2017.
  */
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext}

object WindowWordCount {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("WindowWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 创建StreamingContext
    val ssc = new StreamingContext(sc, Seconds(2))
    // 定义checkpoint目录为当前目录
    ssc.checkpoint(".")


    val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_ONLY_SER)
    val words = lines.flatMap(_.split(" "))

    //countByWindowcountByWindow方法计算基于滑动窗口的DStream中的元素的数量。
    val countByWindow=words.countByWindow(Seconds(4), Seconds(4))

    countByWindow.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
