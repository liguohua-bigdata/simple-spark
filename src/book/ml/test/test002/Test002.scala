package ml.test.test002

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by liguohua on 06/12/2016.
  */
object Test002 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("itemBase")
    val sc = new SparkContext(conf)
    //数据清洗
    val dataClean = sc.textFile("file:///Users/liguohua/Documents/F/code/idea/git/simple-spark/src/pa4/test002/recommend.txt").map { line =>
      val tokens = line.split(",")
      (tokens(0).toLong, (tokens(1).toLong, if (tokens.length > 2) tokens(2).toFloat else 0f))
    }.aggregateByKey(Array[(Long, Float)]())(_ :+ _, _ ++ _).filter(_._2.size > 2).values.persist(StorageLevel.MEMORY_ONLY_SER)


    //全局计算模
    val norms = dataClean.flatMap(_.map(y => (y._1, y._2 * y._2))).reduceByKey(_ + _)

    //广播数据
    val normsMap = sc.broadcast(norms.collectAsMap())
    //共生矩阵
    val matrix = dataClean.map(list => list.sortWith(_._1 > _._1)).flatMap(occMatrix).reduceByKey(_ + _)
    //计算相似度
    val similarity = matrix.map(a => (a._1._1, (a._1._2, 1 / (1 + Math.sqrt(normsMap.value.get(a._1._1).get
      + normsMap.value.get(a._1._2).get - 2 * a._2)))))
    similarity.collect().foreach(println)
    sc.stop
  }

  def occMatrix(a: Array[(Long, Float)]): ArrayBuffer[((Long, Long), Float)] = {
    val array = ArrayBuffer[((Long, Long), Float)]()
    //笛卡尔共生
    for (i <- 0 to (a.size - 1); j <- (i + 1) to (a.size - 1)) {
      array += (((a(i)._1, a(j)._1), a(i)._2 * a(j)._2))
    }
    array

  }

}
