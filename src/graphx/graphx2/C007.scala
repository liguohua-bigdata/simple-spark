package graphx2

import graphx.common.utils.LoggerSetter
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liguohua on 2017/4/6.
  */
object C007 {
  val K = 3
  var arr = new Array[(Int, Int)](K)

  def main(args: Array[String]): Unit = {
    //0.创建运行环境
    LoggerSetter.setLoggerOff()
    val conf = new SparkConf().setAppName("gettingStart").setMaster("local[4]")
    val sc = new SparkContext(conf)


    // Import random graph generation library
    // Create a graph with "age" as the vertex property.  Here we use a random graph for simplicity.
    val graph: Graph[Double, Int] =
    GraphGenerators.logNormalGraph(sc, numVertices = 10).mapVertices((id, _) => id.toDouble)
    // Compute the number of older followers and their total age
    println("Graph:");
    println("sc.defaultParallelism:" + sc.defaultParallelism);
    println("vertices:");
    graph.vertices.foreach(println(_))
    println("edges:");
    graph.edges.foreach(println(_))
    println("count:" + graph.edges.count);
    println("\ninDegrees");
    graph.inDegrees.foreach(println)



    for (i <- 0 until K) {
      arr(i) = (0, 0)
    }

    // Define a reduce operation to compute the highest degree vertex
    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }

    // Define a reduce operation to compute the highest degree vertex
    def min(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 < b._2) a else b
    }
    def minInt(a: (Int, Int), b: (Int, Int)): (Int, Int) = {
      if (a._2 < b._2) a else b
    }

    println("\ntopK:K=" + K);
    def topK(a: (VertexId, Int)): Unit = {
      if (a._2 >= arr.reduce(minInt)._2) {
        arr = arr.sortBy(_._2).reverse
        var tmp = (a._1.toInt, a._2)
        var flag = true
        for (i <- 0 until arr.length) {
          if (a._2 >= arr(i)._2) { //newest max,remove = and last max
            if (flag == true) {
              for (j <- i + 1 until arr.length reverse) {
                arr(j) = arr(j - 1)
              }
              arr(i) = tmp
            }
            flag = false
          }
        }
      }
    }

    graph.inDegrees.foreach(topK(_))
    arr.foreach(println)
    sc.stop()
  }
}
