package book.sparksql.sparksession

import book.utils.MasterUrl
import org.apache.spark.sql.SparkSession

/**
  * Created by liguohua on 07/12/2016.
  */
object CatalogExample {
  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .master(MasterUrl.remoteHA)
      .enableHiveSupport()
      .appName("CatalogExample")
      .getOrCreate()


    val df = sparkSession.read.csv("hdfs://qingcheng12:9000/input/spark/sales.csv")
    df.createTempView("sales")

    //interacting with catalogue
    val catalog = sparkSession.catalog

    //print the databases-->default
    catalog.listDatabases().select("name").show()

    // print all the tables-->sale
    catalog.listTables().select("name").show()

    // is cached-->false
    println(catalog.isCached("sales"))
    df.cache()
    // is cached-->true
    println(catalog.isCached("sales"))

    // drop the table
    catalog.dropTempView("sales")
    catalog.listTables().select("name").show()

    // list functions
    catalog.listFunctions().select("name", "description", "className", "isTemporary").show(500)

    sparkSession.stop()
  }
}
