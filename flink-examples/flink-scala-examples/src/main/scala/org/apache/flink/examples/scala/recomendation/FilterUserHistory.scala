package org.apache.flink.examples.scala.recomendation

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
 * Created by jjoon on 15. 7. 2.
 */

object FilterUserHistory{

  def main (args: Array[String]) {


    val env = ExecutionEnvironment.getExecutionEnvironment
    var inputPath = "/home/jjoon/flink/flink-examples/flink-scala-examples/src/main/scala/org/apache/flink/examples/scala/recomendation/datazal.txt"

    val userData: DataSet[String] = env.readTextFile(inputPath)

    //val saleFilterData = userData.filter(_.contains("SALE")).map(t => t)
    //val userFilterData = saleFilterData.map(
    val userFilterData = userData.map(
      new MapFunction[String, (String,String)] {

        override def map(in: String) = {

          val info: Array[String] = in.split("\t")

          val user = info(1)
          val products: Array[String] = info(3).split(",")
          (user, products(0))


        }
      })
      // user grouping
      .groupBy(0)
      .reduce((t1,t2) => (t1._1, t1._2 )) // To combine all bought products

    }
}


class FilterUserHistory {

}
