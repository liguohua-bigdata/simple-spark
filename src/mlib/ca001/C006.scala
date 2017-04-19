package ca001

import ca001.data.CurrentPath
import graphx.common.utils.LoggerSetter
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 拟合度检验要求输入为Vector, 独立性检验要求输入是Matrix
  */
object C006 {
  def main(args: Array[String]): Unit = {
    LoggerSetter.setLoggerOff()
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("UserCF")
    val sc = new SparkContext(sparkConf)
    //萼片长度，萼片宽度，花瓣长度和花瓣宽度,所属类别
    val data0 = sc.textFile(CurrentPath.currentDataPath + "iris.data")
    val data = data0.map(_.split(",")).map(p => Vectors.dense(p(0).toDouble, p(1).toDouble, p(2).toDouble, p(3).toDouble))
    println("*************************************************")
    //适合度检验 Goodness fo fit
    val v1 = data.first()
    val goodnessOfFitTestResult = Statistics.chiSqTest(v1)
    println(goodnessOfFitTestResult)

    /**
      * method: 方法。这里采用pearson方法。
      *
      * statistic： 检验统计量。简单来说就是用来决定是否可以拒绝原假设的证据。检验统计量的值是利用样本数据计算得到的，它代表了样本中的信息。检验统计量的绝对值越大，拒绝原假设的理由越充分，反之，不拒绝原假设的理由越充分。
      *
      * degrees of freedom：自由度。表示可自由变动的样本观测值的数目，
      *
      * pValue：统计学根据显著性检验方法所得到的P 值。一般以P < 0.05 为显著， P<0.01 为非常显著，其含义是样本间的差异由抽样误差所致的概率小于0.05 或0.01。
      */


    println("*************************************************")
    //卡方独立性检验是用来检验两个属性间是否独立。其中一个属性做为行，另外一个做为列，通过貌似相关的关系考察其是否真实存在相关性。比如天气温变化和肺炎发病率。
    val v2 = data.take(2).last
    val mat: Matrix = Matrices.dense(2, 2, Array(v1(0), v1(1), v2(0), v2(1)))
    val a = Statistics.chiSqTest(mat)
    println(a)


    println("*************************************************")
    //可以把v1作为样本，把v2作为期望值，进行卡方检验：
    val c1 = Statistics.chiSqTest(v1, v2)
    println(c1)


    println("*************************************************")
    //键值对也可以进行独立性检验，这里我们取iris的数据组成键值对：
    val obs = data0.map { line =>
      val parts = line.split(',')
      LabeledPoint(
        if (parts(4) == "Iris-setosa")
          0.toDouble
        else if (parts(4) == "Iris-versicolor")
          1.toDouble
        else
          2.toDouble,
        Vectors.dense(parts(0).toDouble, parts(1).toDouble, parts(2).toDouble, parts(3).toDouble))
    }
    val featureTestResults = Statistics.chiSqTest(obs)
    var i = 1
    featureTestResults.foreach { result =>
      println(s"Column $i:\n$result")
      i += 1
    }

    println("*************************************************")
    //spark也支持Kolmogorov-Smirnov 检验
    val test = data0.map(_.split(",")).map(p => p(0).toDouble)
    val testResult = Statistics.kolmogorovSmirnovTest(test, "norm", 0, 1)

    println(testResult)
  }
}
