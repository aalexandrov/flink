package org.apache.flink.ml.recommendation

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._


/**
 * Created by vassil on 14.06.15.
 */

object AssociationRuleMining {

  private var fileOutput: Boolean = false
  private var textPath: String = null
  private var outputPath: String = null
  private var candidata_support: Int = 2


  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = getTextDataSet(env)

    // Frequent Items

    ////////////////////  Frequent Items START /////////////////////////////////////////////
    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)
    ////////////////////  Frequent Items END ///////////////////////////////////////////////



    //////////////////// Candidate Items START /////////////////////////////////////////////
    val candidateResult = counts
    //////////////////// Candidate Items END ///////////////////////////////////////////////


    //////////////////// Association Rule START ////////////////////////////////////////////
    // Data at that point is in the following form
    // A B 4
    // A C 3
    // A B C 3
    // A C E 1
    // ...


    val expectedResultDummy: Seq[(String)] = {
      Seq(
        ("A B 5"),
        ("A B C 3"),
        ("A C 4"),
        ("A B F 1"),
        ("A C F 1")

      )
    }

    //val associationResul = candidateResult
    val associationResult = expectedResultDummy
    // TODO Vassil implement me




    //////////////////// Association Rule END //////////////////////////////////////////////

    if (fileOutput) {
      counts.writeAsCsv(outputPath, "\n", " ")
    } else {
      counts.print()
    }

    //env.execute("Scala AssociationRule Example")
  }



  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length > 0) {
      fileOutput = true
      if (args.length == 2) {
        textPath = args(0)
        outputPath = args(1)
        true
      } else {
        System.err.println("Usage: AssociationRule <text path> <result path>")
        false
      }
    } else {
      System.out.println("Executing AssociationRule example with built-in default data.")
      System.out.println("  Provide parameters to read input data from a file.")
      System.out.println("  Usage: AssociationRule <text path> <result path>")
      true
    }
  }

  private def getTextDataSet(env: ExecutionEnvironment): DataSet[String] = {
    if (fileOutput) {
      env.readTextFile(textPath)
    }
    else {
      env.fromCollection(RecommendationData.ITEMS)
    }
  }

}


class AssociationRuleMining {


}

