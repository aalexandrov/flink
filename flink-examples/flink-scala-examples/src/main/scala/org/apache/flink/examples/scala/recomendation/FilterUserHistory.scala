package org.apache.flink.examples.scala.recomendation

import org.apache.flink.api.common.functions.{MapFunction, RichFlatMapFunction, RichMapFunction, FlatMapFunction}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector

object FilterUserHistory {

  def main(args: Array[String]) {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val inputPath = "/home/vassil/workspace/flink2/flink/flink-examples/flink-scala-examples/src/main/scala/org/apache/flink/examples/scala/recomendation/datazal.txt"
    //val inputPath = "/home/jjoon/flink/flink-examples/flink-scala-examples/src/main/scala/org/apache/flink/examples/scala/recomendation/datazal.txt"
    val outputPath = "/home/vassil/workspace/inputOutput/output/zalandoProject"
    // val outputPath

    val userData: DataSet[String] = env.readTextFile(inputPath)

    val userFilterData = userData
      .filter(_.contains("SALE"))
      .flatMap(

      new FlatMapFunction[String, (String,String)] {

        def flatMap(in: String, out: Collector[Tuple2[String, String]]) = {

          val info: Array[String] = in.split("\t")

          val user = info(1)
          val products: List[String] = info(3).split(",").toList

          for (product <- products) {
            out.collect((user, product))

          }
        }
      }).distinct
      // user grouping
      .groupBy(0)
      .reduce((t1,t2) => (t1._1, t1._2 + " " + t2._2)) // To combine all bought products

    userFilterData.writeAsText(outputPath , WriteMode.OVERWRITE)

    env.execute("Scala AssociationRule Example")
  }


}

class FilterUserHistory {

}
