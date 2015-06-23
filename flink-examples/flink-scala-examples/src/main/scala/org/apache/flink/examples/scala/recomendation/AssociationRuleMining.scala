package org.apache.flink.examples.scala.recomendation

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode

object AssociationRuleMining {

  private var inputFilePath: String = "/home/vassil/Documents/Studium/Master/IMPRO3/InOut/input/items.txt"
  private var outputFilePath: String = "/home/vassil/Documents/Studium/Master/IMPRO3/InOut/output"
  private var maxIterations: String = "5"
  private var minSupport: String = "4"
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

    // 0) FrequentItem Function
    val input = parseText(text)
    printf("Input String: %s\n", input.collect)

    run(input, outputFilePath, maxIterations.toInt, minSupport.toInt, kPath.toInt)


    // Vassil: In my oppinion the implementation of our next step should be here
    // We have to get the info of the created files and use it for generating of rules

    env.execute("Scala AssociationRule Example")
  }

  private def run(parsedInput:DataSet[String], output:String, maxIterations:Int, minSup:Int, k:Int): Unit =
  {
    var kTemp = k
    var hasConverged = false
    //var preRules:Array[String] = null
    var preRules : Array[Tuple2[String, Int]] = null

    var arrOutput = scala.collection.mutable.ListBuffer.empty[String]
    // According to how much K steps are, Making Pruned Candidate Set
    while (kTemp < maxIterations && !hasConverged) {
      printf("Starting K-Path %s\n", kTemp)

      val candidateRules = findCandidates(parsedInput, preRules, kTemp, minSup)
      val tempRules = candidateRules.collect.toArray
      val cntRules = tempRules.length

      /*
        Here could be AssociationRule Function compared to bible-aprioi.java code
        Probably you can implement the confidence and the interest after pruning (global variable minSupport)
        By using tempRules, DataSet[String]
      */




      /* This was initial not distributed implementation

      if (kTemp >= 2) {

        //candidateRules.joinWithTiny(preRules)

        // Iterate over rules from previous iteration
        for (preRule <-preRules) {
          println("PRE RULE: " + preRule)

          // Get all the rules from current iteration that contain all items of the current preRule
          for( tempRule <- tempRules){
            var containsAllItems = true
            for (item <- preRule._1.toArray){

              //if (!rule.contains(item)) {
              if (!tempRule._1.contains(item)) {
                containsAllItems = false
              }
            }
            if (containsAllItems) {

             //Calculate the confidence here
              println("    RULE: " + tempRule + " CONF: " + tempRule._2+ "/" + preRule._2 + "=" + tempRule._2 / preRule._2.toDouble )
            }
on
          }
        }
      }
      */

      if (kTemp >= 2) {

        var confidences = scala.collection.mutable.ListBuffer.empty[(String,String, Double)]

        //var confidences : Array[Tuple3[String, String, Double]] = new Array[(String, String, Double)](1000)

        // Iterate over rules from previous iteration
        for (preRule <-preRules) {
          println("PRE RULE: " + preRule)

          var output = tempRules
            .filter {item =>  containsAllFromPreRule(item._1, preRule._1)}
            .map {input =>

              println("    RULE: " + input + " CONF: " + input._2+ "/" + preRule._2 + "=" + input._2 / preRule._2.toDouble )

            }

          //TODO Do it tistributed here
          // TODO Aggregate output and write in a file
          //confidences.+=(output)

        }

      }

      if (0 == cntRules) {
        hasConverged = true
      } else {
        preRules = tempRules

        arrOutput += (kTemp + "/" + candidateRules.collect + "\n")

        candidateRules.writeAsText(output + "/" + kTemp, WriteMode.OVERWRITE)
        //candidateRules.writeAsText(output + "/" + kTemp, WriteMode.OVERWRITE)

        kTemp += 1
      }
    }

    printf("Output Candidate:\n")
    arrOutput.foreach(println)
    printf("Converged K-Path %s\n", kTemp)
  }

  def containsAllFromPreRule( newRule: String, preRule: String): Boolean ={

    // TODO do this some other way
    var newRuleCleaned = newRule.replaceAll("\\s+","").replaceAll("[\\[\\](){}]", "")
    var preRuleCleaned = preRule.replaceAll("\\s+","").replaceAll("[\\[\\](){}]", "")

    var newRuleArray = newRuleCleaned.split(",")
    var preRuleArray = preRuleCleaned.split(",")

    var containsAllItems = true

    // Implement that in the filter function
    for (itemOfRule <- preRuleArray){
      if (!newRuleArray.contains(itemOfRule)) {
        containsAllItems = false
      }

    }

    return containsAllItems
  }

  def findCandidates(candidateInput: DataSet[String], prevRules: Array[Tuple2[String, Int]], k:Int, minSup:Int):DataSet[Tuple2[String, Int]] = {
    // 1) Generating Candidate Set Depending on K Path
    candidateInput.flatMap { itemset =>
      val cItem1: Array[Int] = itemset.split(parseContents).map(_.toInt).sorted
      val combGen1 = new CombinationGenerator();
      val combGen2 = new CombinationGenerator();

      var candidates = scala.collection.mutable.ListBuffer.empty[(String,Int)]
      combGen1.reset(k,cItem1)

      while (combGen1.hasMoreCombinations()) {
        val cItem2 = combGen1.next();

        // We assure that the elements will be added in the first itteration. (There are no preRules to compare)
        var valid = true
        if (k > 1) {
          combGen2.reset(k-1,cItem2);
          // Check if the preRules contain all items of the combGenerator
          while (combGen2.hasMoreCombinations() && valid) {
            var nextComb = java.util.Arrays.toString(combGen2.next())

            // valid = prevRules.contains(nextComb)

            var containsItem = false
            for (prevRule <- prevRules) {
              if (prevRule._1.equals(nextComb)) {
                containsItem = true
              }
            }

            valid = containsItem

          }

        }
        // If they contain
        if (valid) {
          candidates += Tuple2(java.util.Arrays.toString(cItem2),1)
        }
      }
      candidates
    } // 2) Merge Candidate Set on Each Same Word
      .groupBy(0).reduce( (t1, t2) => (t1._1, t1._2 + t2._2) )
      // 3) Pruning Step
      .filter(_._2 >= minSup)
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

