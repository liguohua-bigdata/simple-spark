package graphx.c002

import graphx.common.utils.LoggerSetter
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by liguohua on 2017/4/5.
  * 参考链接： https://sanwen8.cn/p/444lCwD.html
  */
object Graphx002 {
  def main(args: Array[String]): Unit = {
    LoggerSetter.setLoggerOff()
    //设置运行环境
    val conf = new SparkConf().setAppName("Graphx002").setMaster("local")
    val sc = new SparkContext(conf)
    println("1.创建点*******************************************")
    val vertexArray = Array(
      (1L, ("Zhang Fei", "Unicom", "Male", 36)),
      (2L, ("Li Zhi", "CMCC", "Female", 18)),
      (3L, ("He Ruina", "Unicom", "Male", 23)),
      (4L, ("Dong Xicheng", "CMCC", "Male", 42)),
      (5L, ("Bai Zi", "Unicom", "Male", 25)),
      (6L, ("Fu Ming", "Unicom", "Male", 50)),
      (7L, ("Handel", "ATT", "Male", 78)),
      (8L, ("Carmen", "ATT", "Female", 21))
    )
    val vertexRDD: RDD[(Long, (String, String, String, Int))] = sc.parallelize(vertexArray)
    vertexRDD.foreach(println(_))

    println("2.创建边*******************************************")
    val edgeArray = Array(
      Edge(2L, 1L, 4),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 5),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 2),
      Edge(7L, 6L, 9),
      Edge(7L, 3L, 8),
      Edge(8L, 7L, 3),
      Edge(8L, 6L, 2)
    )
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
    edgeRDD.foreach(println)
    println("3.创建图*******************************************")
    val telecommGraph: Graph[(String, String, String, Int), Int] = Graph(vertexRDD, edgeRDD)
    println("3.1图操作：查看点*******************************************")
    telecommGraph.vertices.foreach(println)
    println("3.2图操作：查看边*******************************************")
    telecommGraph.edges.foreach(println)
    println("3.3图操作：查看元组*******************************************")
    telecommGraph.triplets.foreach(println)
    println("3.4图操作：图的一般统计信息*******************************************")
    println("点个数为" + telecommGraph.numVertices)
    println("边个数为" + telecommGraph.numEdges)
    println("出度为" + telecommGraph.outDegrees)
    println("入度为" + telecommGraph.inDegrees)
    println("度为" + telecommGraph.degrees)

    println("3.5图操作：求子图*******************************************")
    //从上面的电信网络中，筛选出只包含通话次数超过5次的边和顶点，构建子图
    val sub1 = telecommGraph.subgraph(epred = e => e.attr > 5)
    sub1.triplets.foreach(println)
    println
    //筛选原图中通话双方均为为联通用户，且通话次数在1次以上的边和节点，构建子图
    val sub2 = telecommGraph.subgraph(vpred = (id, vd) => vd._2 == "Unicom", epred = e => e.attr > 1)
    sub2.triplets.foreach(println)
    println
    println("3.6图操作：属性转换*******************************************")
    //使用mapVerices函数对原图坐转换，转换后的节点只包含用户年龄属性
    val newGraph = telecommGraph.mapVertices((id, vd) => vd._4)
    newGraph.vertices.foreach(println)
    println
  }
}
