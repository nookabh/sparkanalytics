package com.workflow.pipeline.example
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.util.MLUtils

object SampleLinearReg {
  def main(arg: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils");
    var jobName = "TransformationMaster"
    var conf: SparkConf = null
    conf = new SparkConf().setMaster("local[10]").setAppName(jobName).set("spark.executor.memory", "1g").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // Load training data in LIBSVM format.
    val training = sqlContext.read.format("libsvm")
      .load("C:\\Users\\nookabh\\Desktop\\Jupyter_Notebook\\trainingdata\\sample_linear.txt")

    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
    // Fit the model
    val lrModel = lr.fit(training)

    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // Summarize the model over the training set and print out some metrics
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")
  }
}