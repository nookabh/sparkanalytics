package com.workflow.pipeline.example
import org.apache.spark.mllib.classification.{ SVMModel, SVMWithSGD }
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object LinearReg {
  def main(arg: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils");
    var jobName = "TransformationMaster"
    var conf: SparkConf = null
    conf = new SparkConf().setMaster("local[10]").setAppName(jobName).set("spark.executor.memory", "1g").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // Load training data in LIBSVM format.

    val data = MLUtils.loadLibSVMFile(sc, "C:\\Users\\nookabh\\Desktop\\Jupyter_Notebook\\trainingdata\\sample_libsvm_data.txt")

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val numIterations = 100
    val model = SVMWithSGD.train(training, numIterations)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)

    // Save and load model
    model.save(sc, "C:\\Users\\nookabh\\Desktop\\Jupyter_Notebook\\trainingdata\\myModelPath")
    val sameModel = SVMModel.load(sc, "C:\\Users\\nookabh\\Desktop\\Jupyter_Notebook\\trainingdata\\myModelPath")
    
    import org.apache.spark.mllib.optimization.L1Updater
    val svmAlg = new SVMWithSGD()
svmAlg.optimizer.
  setNumIterations(200).
  setRegParam(0.1).
  setUpdater(new L1Updater)
val modelL1 = svmAlg.run(training)
println(modelL1)
  }
}