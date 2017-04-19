package graphx2

import graphx.common.utils.LoggerSetter
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liguohua on 2017/4/6.
  * http://blog.csdn.net/xubo245/article/details/51307037
  */
object C002 {
  def main(args: Array[String]): Unit = {
    //0.创建运行环境
    LoggerSetter.setLoggerOff()
    val conf = new SparkConf().setAppName("gettingStart").setMaster("local[4]")
    val sc = new SparkContext(conf)
    println("1.创建点*************************************************")
    val users: RDD[(VertexId, (String, String))] = sc.parallelize(Array(
      (3L, ("rxin", "student")),
      (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "prof")),
      (2L, ("istoica", "prof"))))
    println("2.创建边*************************************************")
    val relationships: RDD[Edge[String]] = sc.parallelize(Array(
      Edge(3L, 7L, "collab"),
      Edge(5L, 3L, "advisor"),
      Edge(2L, 5L, "colleague"),
      Edge(5L, 7L, "pi")))
    println("3.创建默认点*************************************************")
    val defaultUser = ("John Doe", "Missing")
    println("4.创建图*************************************************")
    val graph = Graph(users, relationships, defaultUser)
    println("   4.1图操作：点操作*************************************************")
    val facts2: RDD[String] = graph.triplets.map(
      triplet => {
        triplet.srcId + "(" + triplet.srcAttr._1 + " " + triplet.srcAttr._2 + ")" + " is the" + triplet.attr + " of " + triplet.dstId + "(" + triplet.dstAttr._1 + " " + triplet.dstAttr._2 + ")"
      })
    facts2.collect.foreach(println(_))
    sc.stop()
  }

}
