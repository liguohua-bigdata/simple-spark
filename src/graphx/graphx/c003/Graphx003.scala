package graphx.c003

import graphx.common.utils.LoggerSetter
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liguohua on 2017/4/6.
  */
object Graphx003 {
  def main(args: Array[String]): Unit = {
    LoggerSetter.setLoggerOff()
    //设置运行环境
    val conf = new SparkConf().setAppName("Graphx003").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val p0 = "/Users/liguohua/Documents/F/code/idea/git/simple-spark/src/test/graphx/c003/Cit-HepTh.txt"
    val graph = GraphLoader.edgeListFile(sc, p0)
    graph.vertices.foreach(println)
    println
    graph.edges.foreach(println)
    println
    graph.triplets.foreach(println)
    println
    graph.vertices.take(10).foreach(println)
    println
    val v = graph.pageRank(0.001).vertices.take(5)
    v.foreach(println)
    println
    val v2 = v.reduce((a, b) => if (a._2 > b._2) a else b)
    v.foreach(println)
    println
  }
}
