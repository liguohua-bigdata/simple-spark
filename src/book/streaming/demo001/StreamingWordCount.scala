package streaming.demo001

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by liguohua on 24/02/2017.
  */
object StreamingWordCount {
  def main(args: Array[String]) {
    //创建SparkConf对象
//    val sparkConf = new SparkConf().setAppName("HdfsWordCount").setMaster("spark://qingcheng12:7077")
    val sparkConf = new SparkConf().setAppName("HdfsWordCount").setMaster("local[2]")
    // Create the context
    //创建StreamingContext对象，与集群进行交互
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    //如果目录中有新创建的文件，则读取
//    val lines = ssc.textFileStream("hdfs://qingcheng11:9000/input/spark/people.txt")
    val lines = ssc.textFileStream("file:///Users/liguohua/Documents/F/code/idea/git/simple-spark/book/灵感.md")
    //分割为单词
    val words = lines.flatMap(_.split(" "))
    //统计单词出现次数
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    //打印结果
    wordCounts.print()
    //启动Spark Streaming
    ssc.start()
    //一直运行，除非人为干预再停止
    ssc.awaitTermination()
  }
}
