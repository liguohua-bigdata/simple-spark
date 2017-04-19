package graphx.c001

import graphx.common.utils.LoggerSetter
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext};

/**
  * Created by liguohua on 24/03/2017.
  */
object Graphx001 {

  def main(args: Array[String]): Unit = {
    LoggerSetter.setLoggerOff()
    //设置运行环境
    val conf = new SparkConf().setAppName("Graphx001").setMaster("local")
    val sc = new SparkContext(conf)

    println("1.创建点*******************************************")
    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
    sc.parallelize(Array(
      (3L, ("rxin", "student")),
      (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "prof")),
      (2L, ("istoica", "prof"))))
    users.foreach(println(_))

    println("2.创建默认点*******************************************")
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    println(defaultUser)

    println("3.创建边*******************************************")
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
    sc.parallelize(Array(
      Edge(3L, 7L, "collab"),
      Edge(5L, 3L, "advisor"),
      Edge(2L, 5L, "colleague"),
      Edge(5L, 7L, "pi")))
    relationships.foreach(println(_))

    println("3.创建图*******************************************")
    // Build the initial Graph
    val graph = Graph(users, relationships)
    println("   3.1点*******************************************")
    graph.vertices.foreach(println(_))
    graph.vertices.count()
    println("   3.2边*******************************************")
    graph.edges.foreach(println(_))
    println("   3.3元组*******************************************")
    graph.triplets.foreach(println(_))

    println("   3.4操作点*******************************************")
    // Count all users which are postdocs
    var vf0 = graph.vertices.filter {
      case (id, (name, pos)) => {
        pos == "postdoc"
      }
    }
    vf0.foreach(println(_))
    println("postdocs users count: " + vf0.count())

    println("   3.5操作边*******************************************")
    // Count all the edges where src > dst
    graph.edges.filter { case Edge(src, dst, prop) => src > dst }.foreach(println(_))
    val ed0 = graph.edges.filter(e => e.srcId > e.dstId)
    val ed1 = graph.edges.filter(e => e.srcId > e.dstId)
    ed0.foreach(println(_))
    println("srcId > dstId edges count: " + ed0.count())
    println("   3.6操作元组*******************************************")
    // Use the triplets view to create an RDD of facts.
    val facts: RDD[String] =
    graph.triplets.map(triplet =>
      triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
    facts.collect.foreach(println(_))
    println("facts.count= " + facts.count())



    println("4.创建子图*******************************************")
    val subGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
    subGraph.vertices.foreach(println)
    println()
    var sstrRdd = subGraph.triplets.map(triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
    sstrRdd.foreach(println)
  }
}
