package com.workflow.pipeline.example

import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.classification.SVMModel

object LogisticReg {
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
      println(training)
      val test = splits(1)
      println(test)
      
      // Run training algorithm to build the model
        val model = new LogisticRegressionWithLBFGS()
          .setNumClasses(10)
          .run(training)
        
        // Compute raw scores on the test set.
        val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
          val prediction = model.predict(features)
          (prediction, label)
        }
        
        // Get evaluation metrics.
        val metrics = new MulticlassMetrics(predictionAndLabels)
        val precision = metrics.precision
        println("Precision = " + precision)
        
        val assembler =  new VectorAssembler()
  .setInputCols(Array("c4"))
  .setOutputCol("features")
  
        // Save and load model
        model.save(sc, "C:\\Users\\nookabh\\Desktop\\Jupyter_Notebook\\trainingdata\\mymodel")
//        val sameModel = LogisticRegressionModel.load(sc, "C:\\Users\\nookabh\\Desktop\\Jupyter_Notebook\\trainingdata\\mymodel")
        val sameModel = SVMModel.load(sc, "C:\\Users\\nookabh\\Desktop\\Jupyter_Notebook\\trainingdata\\mymodel")

  }
}