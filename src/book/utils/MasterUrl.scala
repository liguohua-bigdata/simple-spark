package book.utils

/**
  * Created by liguohua on 07/12/2016.
  */

object MasterUrl {
  //本地开启一个
  val local = "local"
  val local1 = "local[1]"
  val local2 = "local[2]"
  val localAll = "local[*]"
  val remoteNonHA = "spark://qingcheng11:7077"
  val remoteHA = "spark://qingcheng11:7077,qingcheng12:7077"
}