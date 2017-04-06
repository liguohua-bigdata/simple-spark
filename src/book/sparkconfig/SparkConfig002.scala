package book.sparkconfig

import org.apache.spark.{SparkConf, SparkContext}

object SparkConfig002 {
  def main(args: Array[String]): Unit = {
    case class Student(name: String, age: Int)
    case class Teacher(name: String, age: Int)

    //1.创建配置文件
    val sparkConf = new SparkConf()
    sparkConf.setAppName(this.getClass.getName)
    sparkConf.set("spark.logConf", "true")
//    sparkConf.setMaster("local[*]")
    sparkConf.setMaster("spark://qingcheng11:7077")
    //2.注册serializer，默认为org.apache.spark.serializer.JavaSerialization，通用但性能低
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerialization")
    //3.配置KryoClasses类
    sparkConf registerKryoClasses (Array(classOf[Student], classOf[Teacher]))
    //4.创建sparkContext
    val spark = new SparkContext(sparkConf)
    //5.定义RDD[Student]
    val stu = spark.parallelize(Seq(Student("zhangsan", 15), Student("lisi", 16)))
    stu.foreach(println(_))
    //5.定义RDD[Teacher]
    val tea = spark.parallelize(Seq(Teacher("dahua", 55), Teacher("daming", 46)))
    tea.foreach(println(_))
    //6.关闭sparkcontext
    if (!spark.isStopped) {
      spark.stop()
    }
  }
}
