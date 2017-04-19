package ca001

import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
  * Created by liguohua on 2017/4/19.
  */
object c001 {
  def main(args: Array[String]): Unit = {
    // 创建一个稠密本地向量
    val dv: Vector = Vectors.dense(2.0, 0.0, 8.0)
    // 创建一个稀疏本地向量
    // 方法第二个参数数组指定了非零元素的索引，而第三个参数数组则给定了非零元素值
    val sv1: Vector = Vectors.sparse(3,
      Array(0, 2),
      Array(2.0, 8.0))
    // 另一种创建稀疏本地向量的方法
    // 方法的第二个参数是一个序列，其中每个元素都是一个非零值的元组：(index,elem)
    val sv2: Vector = Vectors.sparse(3, Seq((0, 2.0), (2, 8.0)))
  }

}
