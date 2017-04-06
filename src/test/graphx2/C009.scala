package graphx2

import graphx.common.utils.LoggerSetter
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.util.GraphGenerators

/**
  * Created by liguohua on 2017/4/6.
  */
object C009 {
  def main(args: Array[String]): Unit = {
    //0.创建运行环境
    LoggerSetter.setLoggerOff()
    val conf = new SparkConf().setAppName("gettingStart").setMaster("local[4]")
    val sc = new SparkContext(conf)
    // A graph with edge attributes containing distances  
    val graph: Graph[Long, Double] =
    GraphGenerators.logNormalGraph(sc, numVertices = 5).mapEdges(e => e.attr.toDouble)
    val sourceId: VertexId = 2 // The ultimate source  
    // Initialize the graph such that all vertices except the root have distance infinity.  

    println("graph:")
    println("vertices:")
    graph.vertices.foreach(println)
    println("edges:")
    graph.edges.foreach(println)
    println()

    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
    println("initialGraph:")
    println("vertices:")
    initialGraph.vertices.foreach(println)
    println("edges:")
    initialGraph.edges.foreach(println)
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program  
      triplet => {
        // Send Message  
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message  
    )
    println()
    println("sssp:")
    println("vertices:")
    println(sssp.vertices.toString())
    println("edges:")
    sssp.edges.foreach(println)
    sc.stop()
  }
}
