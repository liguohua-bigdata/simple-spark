package graphx2

import graphx.common.utils.LoggerSetter
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{EdgeDirection, Graph}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liguohua on 2017/4/6.
  */
object C008a {
  def main(args: Array[String]): Unit = {
    //0.创建运行环境
    LoggerSetter.setLoggerOff()
    val conf = new SparkConf().setAppName("gettingStart").setMaster("local[4]")
    val sc = new SparkContext(conf)
    // Import random graph generation library  
    // Create a graph with "age" as the vertex property.  Here we use a random graph for simplicity.  
    val graph: Graph[Double, Int] =
    GraphGenerators.logNormalGraph(sc, numVertices = 6).mapVertices((id, _) => id.toDouble)
    // Compute the number of older followers and their total age  

    println("Graph:")
    println("sc.defaultParallelism:" + sc.defaultParallelism)
    println("vertices:")
    graph.vertices.collect.foreach(println(_))
    println("edges:")
    graph.edges.collect.foreach(println(_))
    println("count:" + graph.edges.count)
    println("\ninDegrees")
    graph.inDegrees.foreach(println)

    println("\nneighbors0:")
    val neighbors0 = graph.collectNeighborIds(EdgeDirection.Out)
    neighbors0.foreach(println)
    neighbors0.collect.foreach { a => {
      println(a._1 + ":")
      a._2.foreach(b => print(b + " "))
      println()
    }
    }
    sc.stop()
  }
}
