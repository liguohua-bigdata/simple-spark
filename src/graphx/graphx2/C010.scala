package graphx2

import graphx.common.utils.LoggerSetter
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liguohua on 2017/4/6.
  */
object C010 {
  def main(args: Array[String]): Unit = {
    //0.创建运行环境
    LoggerSetter.setLoggerOff()
    val conf = new SparkConf().setAppName("gettingStart").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // Load the edges as a graph
    val graph = GraphLoader.edgeListFile(sc, "/Users/liguohua/Documents/F/code/idea/git/simple-spark/src/test/graphx2/ca001.data/followers.txt")
    // Run PageRank
    val ranks = graph.pageRank(0.0001).vertices
    // Join the ranks with the usernames
    val users = sc.textFile("/Users/liguohua/Documents/F/code/idea/git/simple-spark/src/test/graphx2/ca001.data/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }
    // Print the result
    println(ranksByUsername.collect().mkString("\n"))

    sc.stop()
  }
}
