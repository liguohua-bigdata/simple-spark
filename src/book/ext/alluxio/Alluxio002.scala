package ext.alluxio

import org.apache.spark.{SparkConf, SparkContext}

object Alluxio002 {
  def main(args: Array[String]) {
    System.setProperty("HADOOP_USER_NAME", "root")
    System.setProperty("ALLUXIO_USER_NAME", "root")
    System.setProperty("alluxio.security.authorization.permission.enabled", "false")
    //1.创建spark执行环境
    val conf = new SparkConf().setAppName(this.getClass.getName)
      .setMaster("local[*]")
    val spark = new SparkContext(conf)

    //2.读取alluxio上的文件
    val rdd = spark.parallelize(1 to 10)
        rdd.saveAsTextFile("alluxio://qingcheng11:19998/output/spark/alluxio/test004.txt")
//    rdd.saveAsTextFile("hdfs://qingcheng11:9000/output/spark/alluxio/test002.txt")

    //3.关闭spark执行上下文
    spark.stop()
  }
}