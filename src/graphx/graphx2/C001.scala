package graphx2

import graphx.common.utils.LoggerSetter
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liguohua on 2017/4/6.
  * http://blog.csdn.net/xubo245/article/details/51306975
  */
object C001 {
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
    users.foreach(println)
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
    //点过滤
    val c0 = graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count
    println("c0=" + c0)
    var c1 = graph.edges.filter(e => e.srcId > e.dstId).count
    println("c1=" + c1)
    //边过滤
    c1 = graph.edges.filter { case Edge(src, dst, prop) => src > dst }.count
    println("c1=" + c1)
    //元组操作
    val facts: RDD[String] =
    graph.triplets.map(triplet =>
      triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
    facts.foreach(println)

    sc.stop()
  }

}
