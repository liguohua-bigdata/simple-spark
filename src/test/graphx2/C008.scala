package graphx2

import graphx.common.utils.LoggerSetter
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liguohua on 2017/4/6.
  */
object C008 {
  def main(args: Array[String]): Unit = {
    //0.创建运行环境
    LoggerSetter.setLoggerOff()
    val conf = new SparkConf().setAppName("gettingStart").setMaster("local[4]")
    val sc = new SparkContext(conf)
    // Import random graph generation library
    // Create a graph with "age" as the vertex property.  Here we use a random graph for simplicity.
    val graph: Graph[Double, Int] =
    GraphGenerators.logNormalGraph(sc, numVertices = 5).mapVertices((id, _) => id.toDouble)
    // Compute the number of older followers and their total age

    println("Graph:")
    println("sc.defaultParallelism:" + sc.defaultParallelism)
    println("vertices:")
    graph.vertices.collect.foreach(println(_))
    println("edges:")
    graph.edges.collect.foreach(println(_))
    println("count:" + graph.edges.count)
    println("\ndegrees")
    graph.degrees.foreach(println)
    println("\ninDegrees")
    graph.inDegrees.foreach(println)
    println("\noutDegrees")
    graph.outDegrees.foreach(println)

    // Define a reduce operation to compute the highest degree vertex
    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }
    // Compute the max degrees
    val maxInDegree: (VertexId, Int) = graph.inDegrees.reduce(max)
    val maxOutDegree: (VertexId, Int) = graph.outDegrees.reduce(max)
    val maxDegrees: (VertexId, Int) = graph.degrees.reduce(max)

    println("\nmax:")
    println("maxDegree:" + (graph.degrees.reduce(max)))
    println("maxInDegree:" + graph.inDegrees.reduce(max))
    println("maxoutDegree:" + graph.outDegrees.reduce(max))


    // Define a reduce operation to compute the highest degree vertex
    def min(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 < b._2) a else b
    }
    println("\nmin:")
    println("minDegree:" + (graph.degrees.reduce(min)))
    println("minInDegree:" + graph.inDegrees.reduce(min))
    println("minoutDegree:" + graph.outDegrees.reduce(min))
    sc.stop()
  }
}
