package graphx2

import graphx.common.utils.LoggerSetter
import org.apache.spark.graphx.{GraphLoader, VertexId}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liguohua on 2017/4/6.
  */
object C015 {
  def main(args: Array[String]): Unit = {
    //0.创建运行环境
    LoggerSetter.setLoggerOff()
    val conf = new SparkConf().setAppName("gettingStart").setMaster("local[4]")
    val sc = new SparkContext(conf)


    // Parse the edge ca001.data which is already in userId -> userId format
    val graph = GraphLoader.edgeListFile(sc, "/Users/liguohua/Documents/F/code/idea/git/simple-spark/src/test/graphx2/ca001.data/web-Google.txt")
    println("graph.numEdges:" + graph.numEdges)
    println("graph.numVertices:" + graph.numVertices)
    println("\n edges 10:")
    graph.edges.take(10).foreach(println)
    println("\n vertices 10:")
    graph.vertices.take(10).foreach(println)

    //***************************************************************************************************  
    //*******************************          图的属性          *****************************************  
    //***************************************************************************************************  
    println("**********************************************************")
    println("属性演示")
    println("**********************************************************")
    println("Graph:")

    //Degrees操作  
    println("找出图中最大的出度、入度、度数：")
    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }
    println("max of outDegrees:" + graph.outDegrees.reduce(max) + " max of inDegrees:" + graph.inDegrees.reduce(max) + " max of Degrees:" + graph.degrees.reduce(max))
    println


    sc.stop()
  }
}
