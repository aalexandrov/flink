package org.apache.flink.examples.scala.recomendation

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode

object AssociationRuleMining {

  private var inputFilePath: String = "/home/vassil/Documents/Studium/Master/IMPRO3/InOut/input/items.txt"
  //private var outputFilePath: String = "/home/vassil/Documents/Studium/Master/IMPRO3/InOut/output"
  private var outputFilePath: String = "/home/jjoon/output"
  private var maxIterations: String = "6"
  private var minSupport: String = "3"

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

    run(input, outputFilePath, maxIterations.toInt, minSupport.toInt)

    // Vassil: In my oppinion the implementation of our next step should be here
    // We have to get the info of the created files and use it for generating of rules
    env.execute("Scala AssociationRule Example")
  }

  private def run(parsedInput:DataSet[String], output:String, maxIterations:Int, minSup:Int): Unit =
  {
    var kTemp = 1
    var hasConverged = false
    var preRules : Array[Tuple2[String, Int]] = null

    var arrConfidences = scala.collection.mutable.ListBuffer.empty[(String)]
    var confidenceOutput : DataSet[Array[String]] = null

    // According to how much K steps are, Making Pruned Candidate Set
    while (kTemp < maxIterations && !hasConverged) {
      printf("Starting K-Path %s\n", kTemp)

      val candidateRules :DataSet[Tuple2[String, Int]] = findCandidates(parsedInput, preRules, kTemp, minSup)
      val tempRules = candidateRules.collect.toArray
      val cntRules = tempRules.length
      //Dataset_way
      //val tempRules = candidateRules
      //val cntRules = tempRules.count

      if (kTemp >= 2) {

        // Iterate over rules from previous iteration
        //Dataset_way
        //preRules.map{ preRule =>
        for (preRule <-preRules) {
          val confidences = tempRules.filter {item =>  containsAllFromPreRule(item._1, preRule._1)}
            .map { input =>
              Tuple2(preRule._1 +" => "+ input._1, 100*(input._2 / preRule._2.toDouble))
            //RULE: [2, 6] => [2, 4, 6] CONF RATE: 4/6=66.66
          }
          println("OUTPUT FILE: " + confidences.mkString)
          arrConfidences += confidences.mkString
          //Dataset_way
          //confidenceOutput += confidences
        }
      }

      if (0 == cntRules) {
        hasConverged = true
      } else {
        preRules = tempRules
        // TODO ARRAY[String] -> DATASET[ARRAY[STRING]]
        //Dataset_way
        // I think we should change
        // - findCandidate function from PrevRule : DataSet[Array[Tuple2]]
        // - tempRules : DataSet[Array[Tuple2]]
        //confidenceOutput.writeAsText(output + "/" + kTemp, WriteMode.OVERWRITE)
        arrConfidences

        kTemp += 1
      }
    }
    //printf("Output Candidate:\n")
    printf("Converged K-Path %s\n", kTemp)
  }


  //Dataset_way
  //def findCandidates(candidateInput: DataSet[String], prevRules: DataSet[Array[Tuple2[String, Int]]], k:Int, minSup:Int):DataSet[Tuple2[String, Int]] = {
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


  private def containsAllFromPreRule( newRule: String, preRule: String): Boolean ={

    // TODO do this some other way
    val newRuleCleaned = newRule.replaceAll("\\s+","").replaceAll("[\\[\\](){}]", "")
    val preRuleCleaned = preRule.replaceAll("\\s+","").replaceAll("[\\[\\](){}]", "")

    val newRuleArray = newRuleCleaned.split(",")
    val preRuleArray = preRuleCleaned.split(",")

    var containsAllItems = true

    // Implement that in the filter function
    for (itemOfRule <- preRuleArray){
      if (!newRuleArray.contains(itemOfRule)) {
        containsAllItems = false
      }
    }
    return containsAllItems
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

