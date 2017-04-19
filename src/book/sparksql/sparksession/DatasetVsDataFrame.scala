package book.sparksql.sparksession

/**
  * Created by liguohua on 07/12/2016.
  */

import org.apache.spark.sql.SparkSession

/**
  * Logical Plans for Dataframe and Dataset
  */
object DatasetVsDataFrame {

  case class Sales(transactionId: Int, customerId: Int, itemId: Int, amountPaid: Double)

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .enableHiveSupport()
      .appName(this.getClass.getName)
      .getOrCreate()

    val sparkContext = sparkSession.sparkContext
    import sparkSession.implicits._


    //read ca001.data from text file
    val filePath = "hdfs://qingcheng12:9000/input/spark/sales.csv"
    val df = sparkSession.read.option("header", "true").option("inferSchema", "true").csv(filePath)
    val ds = sparkSession.read.option("header", "true").option("inferSchema", "true").csv(filePath).as[Sales]


    val selectedDF = df.select("itemId")
    val selectedDS = ds.map(_.itemId)

    println(selectedDF.queryExecution.optimizedPlan.numberedTreeString)
    println(selectedDS.queryExecution.optimizedPlan.numberedTreeString)
  }

}