package graphx2

import graphx.common.utils.LoggerSetter
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liguohua on 2017/4/6.
  * http://blog.csdn.net/xubo245/article/details/51307162
  */
object C003 {
  def main(args: Array[String]): Unit = {
    //0.创建运行环境
    LoggerSetter.setLoggerOff()
    val conf = new SparkConf().setAppName("gettingStart").setMaster("local[4]")
    val sc = new SparkContext(conf)
    println("1.创建图*************************************************")
    val users: RDD[(VertexId, (String, String))] = sc.parallelize(Array(
      (3L, ("rxin", "student")),
      (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "prof")),
      (2L, ("istoica", "prof"))))
    val relationships: RDD[Edge[String]] = sc.parallelize(Array(
      Edge(3L, 7L, "collab"),
      Edge(5L, 3L, "advisor"),
      Edge(2L, 5L, "colleague"),
      Edge(5L, 7L, "pi")))
    val defaultUser = ("John Doe", "Missing")
    val graph = Graph(users, relationships, defaultUser)
    println("1.1subgraph过滤出满足要求的子图*************************************************")
    val subgraph0 = graph.subgraph(e => (e.srcId % 2) != 0)
    subgraph0.edges.foreach(println(_))
    println

    val subgraph1 = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
    subgraph1.edges.foreach(println(_))

    sc.stop()
  }
}
