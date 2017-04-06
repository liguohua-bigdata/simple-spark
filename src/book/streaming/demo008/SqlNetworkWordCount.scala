package streaming.demo008

/**
  * Created by liguohua on 24/02/2017.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

object SqlNetworkWordCount {
  def main(args: Array[String]) {
    // Create the context with a 2 second batch size
    val sparkConf = new SparkConf().setAppName("SqlNetworkWordCount").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create a socket stream on target ip:port and count the
    // words in input stream of \n delimited text (eg. generated by 'nc')
    // Note that no duplication in storage level only for running locally.
    // Replication necessary in distributed scenario for fault tolerance.
    //Socke作为数据源
    val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    //words DStream
    val words = lines.flatMap(_.split(" "))

    // Convert RDDs of the words DStream to DataFrame and run SQL query
    //调用foreachRDD方法，遍历DStream中的RDD
    words.foreachRDD((rdd: RDD[String], time: Time) => {
      // Get the singleton instance of SQLContext
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._

      // Convert RDD[String] to RDD[case class] to DataFrame
      val wordsDataFrame = rdd.map(w => Record(w)).toDF()

      // Register as table
      wordsDataFrame.registerTempTable("words")

      // Do word count on table using SQL and print it
      val wordCountsDataFrame =
      sqlContext.sql("select word, count(*) as total from words group by word")
      println(s"========= $time =========")
      wordCountsDataFrame.show()
    })

    ssc.start()
    ssc.awaitTermination()
  }
}


/** Case class for converting RDD to DataFrame */
case class Record(word: String)


/** Lazily instantiated singleton instance of SQLContext */
object SQLContextSingleton {

  @transient private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}
