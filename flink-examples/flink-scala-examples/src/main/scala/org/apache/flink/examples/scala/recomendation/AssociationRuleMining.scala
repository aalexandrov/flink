package org.apache.flink.examples.scala.recomendation

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode

object AssociationRuleMining {

  private var inputFilePath: String = "/home/vassil/Documents/Studium/Master/IMPRO3/InOut/input/items.txt"
  private var outputFilePath: String = "/home/vassil/Documents/Studium/Master/IMPRO3/InOut/output"
  private var maxIterations: String = "5"
  private var minSupport: String = "2"
  private var kPath: String = "1"

  // Test Case fileInput = false
  private val fileInput: Boolean = false
  private val parseContents = " "
  private val parseKeyValue = "\t"

  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = getTextDataSet(env)
    val input = parseText(text)

    run(input, outputFilePath, maxIterations.toInt, minSupport.toInt, kPath.toInt)

    env.execute("Scala AssociationRule Example")
  }

  private def run(parsedInput:DataSet[String], output:String, maxIterations:Int, minSup:Int, k:Int): Unit =
  {
    var kTemp = k
    var hasConverged = false
    var preRules:Array[String] = null

    var arrOutput = scala.collection.mutable.ListBuffer.empty[String]

    while (kTemp < maxIterations && !hasConverged) {
      printf("Starting K-Path %s\n", kTemp)

      val candidateRules = findCandidates(parsedInput, preRules, kTemp, minSup)
      val tempRules = candidateRules.collect.toArray
      val cntRules = tempRules.length

      if (0 == cntRules) {
        hasConverged = true
      } else {
        preRules = tempRules

        arrOutput += (kTemp + "/" + candidateRules.collect + "\n")
        candidateRules.writeAsText(output + "/" + kTemp, WriteMode.OVERWRITE)

        kTemp += 1
      }
    }

    printf("Output Candidate:\n")
    arrOutput.foreach(println)
    printf("Converged K-Path %s\n", kTemp)
  }

  def findCandidates(candidateInput: DataSet[String], prevRules: Array[String], k:Int, minSup:Int):DataSet[String] = {
    candidateInput.flatMap { itemset =>

      // To Change Flink One under this function
      val cItem1: Array[Int] = itemset.split(parseContents).map(_.toInt).sorted
      val combGen1 = new CombinationGenerator();
      val combGen2 = new CombinationGenerator();

      var candidates = scala.collection.mutable.ListBuffer.empty[(String,Int)]
      combGen1.reset(k,cItem1)

      // To Change Flink One
      while (combGen1.hasMoreCombinations()) {
        val cItem2 = combGen1.next();
        var valid = true
        if (k > 1) {
          combGen2.reset(k-1,cItem2);
          while (combGen2.hasMoreCombinations() && valid) {
            valid = prevRules.contains(java.util.Arrays.toString(combGen2.next()))
          }
        }

        if (valid) {
          candidates += Tuple2(java.util.Arrays.toString(cItem2),1)
        }

      }
      candidates
    }
      .groupBy(0)
      .reduce( (t1, t2) => (t1._1, t1._2 + t2._2) ).filter(_._2 >= minSup)
      .map{pair => pair._1}
  }

  private def parseText(textInput:DataSet[String]) = {
    textInput.map { input =>

      val idx = input.indexOf(parseKeyValue)
      val key = input.substring(0, idx)
      val value = input.substring(idx + 1, input.length)
      value.split(parseContents).distinct.mkString(parseContents)
    }
  }


  private def parseParameters(args: Array[String]): Boolean = {

    // input, output maxIterations, kPath, minSupport
    if (args.length > 0) {
      if (args.length == 5) {
        inputFilePath = args(0)
        outputFilePath = args(1)
        maxIterations = args(2)
        minSupport = args(3)
        kPath = args(4)
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

    if (fileInput) {
      println("From File")
      env.readTextFile(inputFilePath)
    }
    else {
      println("From Code")
      env.fromCollection(RecommendationData.ITEMS)
    }
  }
}

class AssociationRuleMining {

}

