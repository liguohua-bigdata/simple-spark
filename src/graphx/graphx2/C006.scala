package graphx2

import graphx.common.utils.LoggerSetter
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liguohua on 2017/4/6.
  * http://blog.csdn.net/xubo245/article/details/51307162
  */
object C006 {
  def main(args: Array[String]): Unit = {
    //0.创建运行环境
    LoggerSetter.setLoggerOff()
    val conf = new SparkConf().setAppName("gettingStart").setMaster("local[4]")
    val sc = new SparkContext(conf)
    // Import random graph generation library
    // Create a graph with "age" as the vertex property.  Here we use a random graph for simplicity.
    val graph: Graph[Double, Int] = GraphGenerators.logNormalGraph(sc, numVertices = 5).mapVertices((id, _) => id.toDouble)
    // Compute the number of older followers and their total age
    println("Graph:")
    println("sc.defaultParallelism:" + sc.defaultParallelism)
    println("vertices:")
    graph.vertices.foreach(println)
    println("edges:")
    graph.edges.foreach(println)
    println("triplets:")
    graph.triplets.foreach(println)
    println("edges.count")
    println("count:" + graph.edges.count)
    println("\ninDegrees")
    graph.inDegrees.foreach(println)
    println("\noutDegrees")
    graph.outDegrees.foreach(println)
    println("\nreverse")
    println("\nreverse triplets")
    graph.reverse.triplets.foreach(println)
    println("\nreverse edges")
    graph.reverse.edges.foreach(println)
    println("\nreverse vertices")
    graph.reverse.vertices.foreach(println)
    println("\nreverse inDegrees")
    graph.reverse.inDegrees.foreach(println)
    println("\nreverse inDegrees")
    graph.reverse.outDegrees.foreach(println)
    println(graph.numEdges)
    println(graph.numVertices)
    println(graph.inDegrees)
    println(graph.outDegrees)
    println(graph.degrees)
    sc.stop()
  }
}
