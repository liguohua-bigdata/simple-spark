package pa4

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}

object Pa4ReFine {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CollaborativeFilteringExample")
    conf.setMaster("local[*]")
    //    conf.setMaster(MasterUrl.remoteHA)
    val sc = new SparkContext(conf)

    //1.LOAD DATA
    val itemPath = "file:///Users/liguohua/Documents/F/code/idea/git/simple-spark/src/pa4/items.txt"
    val dataPath = "file:///Users/liguohua/Documents/F/code/idea/git/simple-spark/src/pa4/data.txt"

    val itemLines = sc.textFile(itemPath).distinct()
    val dataLines = sc.textFile(dataPath)

    //2.CONVERT DATA
    case class Product(productId: String, userId: String, score: Double)
    val spliter = ":"
    var productId = ""
    var userId = ""
    var score = 0.0
    val products = dataLines.map(line => {
      if (!line.isEmpty) {
        if (line.startsWith("product/productId")) {
          val tokens = line.split(spliter)
          if (tokens.length == 2) {
            productId = tokens(1).trim
          }
        }
        if (line.startsWith("review/userId")) {
          val tokens = line.split(spliter)
          if (tokens.length == 2) {
            userId = tokens(1).trim
          }
        }
        if (line.startsWith("review/score")) {
          val tokens = line.split(spliter)
          if (tokens.length == 2) {
            score = tokens(1).trim.toDouble
          }
        }
        Product(productId, userId, score)
      } else {
        Product(productId, userId, score)
      }
    })

    //    products.collect().foreach(println(_))

    //3.PURE DATA
    val pureProductes = products.filter(p => {
      (p.productId.nonEmpty && (!p.productId.equalsIgnoreCase("unknown"))) &&
        (p.userId.nonEmpty && (!p.userId.equalsIgnoreCase("unknown"))) &&
        (p.score != 0.0)
    }).distinct()
    //        pureProductes.collect().foreach(println(_))

    val pureItemLines = itemLines.intersection(products.map(p => productId))
    //        pureItemLines.collect().foreach(println(_))


    //3.HASH PURE DATA
    case class ProductHash(productId: String, productIdHash: Int, userId: String, userIdHash: Int, score: Double)
    val pureProductesWithHash = pureProductes.map(p => {
      ProductHash(p.productId, math.abs(p.productId.hashCode), p.userId, math.abs(p.userId.hashCode), p.score)
    })
    //    pureProductesWithHash.collect().foreach(println(_))


    //3.rating DATA
    val ratings = pureProductesWithHash.map { p => Rating(p.productIdHash, p.userIdHash, p.score) }
    val rank = 12
    val lambda = 0.01
    val numIterations = 5 //迭代次数太大需要的内存很多
    val model = ALS.train(ratings, rank, numIterations, lambda)

    //4.变量item文件中内容，对每一个item分别用训练出来的模型进行推荐
    val pureItemLinesList = pureItemLines.collect()
    for (inputProduct <- pureItemLinesList) {
      val K = 10
      val topKRecsRdd = sc.makeRDD(model.recommendProducts(Math.abs(inputProduct.hashCode()), K))
      //    topKRecsRdd.collect().foreach(println(_))


      val topKRecsRddForTuple = topKRecsRdd.map(r => (r.product, r.user)).distinct()
      //    topKRecsRddForTuple.collect().foreach(println(_))

      val pureProductesForTuple = pureProductesWithHash.map(p => (p.productIdHash, p.productId)).distinct()
      //    pureProductesForTuple.collect().foreach(println(_))


      val recomProductRdd = pureProductesForTuple.leftOuterJoin(topKRecsRddForTuple)
      //        r.collect().foreach(println(_))

      val recomProductIdRdd = recomProductRdd.map(_._2._1).distinct()
      //    r1.collect.foreach(println(_))

      var recomProductIdStr = inputProduct
      recomProductIdRdd.collect.foreach(recomProductIdStr += "," + _)
      println(recomProductIdStr)
    }
    //    println("output":+inputProduct+r1.collect)
    sc.stop()
  }

}
