package graphx2

import graphx.common.utils.LoggerSetter
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liguohua on 2017/4/6.
  */
object C013 {
  def main(args: Array[String]): Unit = {
    //0.创建运行环境
    LoggerSetter.setLoggerOff()
    val conf = new SparkConf().setAppName("gettingStart").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // Load the edges as a graph
    val graph = GraphLoader.edgeListFile(sc, "/Users/liguohua/Documents/F/code/idea/git/simple-spark/src/test/graphx2/data/followers.txt")
    // Find the connected components  
    val cc = graph.connectedComponents().vertices
    // Join the ranks with the usernames
    val users = sc.textFile("/Users/liguohua/Documents/F/code/idea/git/simple-spark/src/test/graphx2/data/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ccByUsername = users.join(cc).map {
      case (id, (username, cc)) => (username, cc)
    }
    // Print the result

    println("\ngraph edges")
    println("edges:")
    graph.edges.foreach(println)
    graph.edges.foreach(println)
    println("vertices:")
    graph.vertices.foreach(println)
    println("triplets:")
    graph.triplets.foreach(println)
    println("\nusers")
    users.foreach(println)
    println("\ncc:")
    cc.foreach(println)
    println("\nccByUsername")
    println(ccByUsername.collect().mkString("\n"))

    sc.stop()
  }
}
