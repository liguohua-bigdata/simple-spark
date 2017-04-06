package graphx2

import graphx.common.utils.LoggerSetter
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liguohua on 2017/4/6.
  * http://blog.csdn.net/xubo245/article/details/51307162
  */
object C004 {
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
    println("1.1mask：返回的是current graph和other graph的公共子图图*************************************************")


    // Run Connected Components
    val ccGraph = graph.connectedComponents() // No longer contains missing field
    // Remove missing vertices as well as the edges to connected to them
    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
    // Restrict the answer to the valid subgraph
    val validCCGraph = ccGraph.mask(validGraph)
    println("\nccGraph:")
    println("vertices:")
    ccGraph.vertices.collect.foreach(println)
    println("edegs:")
    ccGraph.edges.collect.foreach(println)
    println("\nvalidGraph:")
    validGraph.vertices.collect.foreach(println)
    println("\nvalidCCGraph:")
    validCCGraph.vertices.collect.foreach(println)



    sc.stop()
  }
}
