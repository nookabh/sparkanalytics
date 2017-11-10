package com.workflow.pipeline.example
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy

object GBTRegressionExample {
  def main(arg: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils");
    var jobName = "TransformationMaster"
    var conf: SparkConf = null
    conf = new SparkConf().setMaster("local[10]").setAppName(jobName).set("spark.executor.memory", "1g").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // Load and parse the data file, converting it to a DataFrame.
//    val data = sqlContext.read.format("libsvm").load("C:\\Users\\nookabh\\Desktop\\Jupyter_Notebook\\trainingdata\\sample_little_libsvm.txt")

    // Load and parse the data file.
    val csvData = sc.textFile("C:\\Users\\nookabh\\Desktop\\Jupyter_Notebook\\trainingdata\\sample_tree_data.csv")
    val data = csvData.map { line =>
      val parts = line.split(',').map(_.toDouble)
      LabeledPoint(parts(0), Vectors.dense(parts.tail))
    }
    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a GBT model.
    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
    boostingStrategy.numIterations = 50
    boostingStrategy.treeStrategy.numClasses = 2
    boostingStrategy.treeStrategy.maxDepth = 6
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

    val model = GradientBoostedTrees.train(trainingData, boostingStrategy)
    
    // Get class label from raw predict function
    val predictedLabels = model.predict(testData.map(_.features))
    predictedLabels.collect

    // Get class probability
    val treePredictions = testData.map { point => model.trees.map(_.predict(point.features)) }
    val treePredictionsVector = treePredictions.map(array => Vectors.dense(array))
    val treePredictionsMatrix = new RowMatrix(treePredictionsVector)
    val learningRate = model.treeWeights
    val learningRateMatrix = Matrices.dense(learningRate.size, 1, learningRate)
    val weightedTreePredictions = treePredictionsMatrix.multiply(learningRateMatrix)
    val classProb = weightedTreePredictions.rows.flatMap(_.toArray).map(x => 1 / (1 + Math.exp(-1 * x)))
    val classLabel = classProb.map(x => if (x > 0.5) 1.0 else 0.0)
    classLabel.collect
  }
}