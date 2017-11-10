package com.workflow.pipeline.example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

object WinePricePredictionApp {

  def main(arg: Array[String]) {

    System.setProperty("hadoop.home.dir", "C:\\winutils");
    var jobName = "TransformationMaster"
    var conf: SparkConf = null
    conf = new SparkConf().setMaster("local[10]").setAppName(jobName).set("spark.executor.memory", "1g").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val schemaStruct = StructType(
      StructField("points", DoubleType) ::
        StructField("country", StringType, false) ::
        StructField("price", DoubleType) :: Nil)

    def getHistoryDF(): DataFrame = {
      var csvInPath = "C:\\Users\\nookabh\\Desktop\\Jupyter_Notebook\\trainingdata\\winelittle.txt"
      var rawTextRDD = sc.textFile(csvInPath)
      rawTextRDD = rawTextRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      val quoteHistorySchemaFields = "id,country,points,price,province,region_1,region_2".split(",")
        .map(fieldName => StructField(fieldName, StringType, nullable = true))
      val quoteHistorySchema = StructType(quoteHistorySchemaFields)
      val rowRDD = rawTextRDD.map(_.split(",")).map(p => {
        if (p.length == 7) {
          val row: Row = Row(p(0), p(1), p(2), p(3), p(4), p(5), p(6))
          row
        } else {
          Row("", "", "", "", "", "", "")
        }
      })
      // Apply the schema to the RDD.
      val history_df = sqlContext.createDataFrame(rowRDD, quoteHistorySchema)
      history_df
    }

    val df = getHistoryDF
    df.show()
     val princeIndexer = new StringIndexer()
      .setInputCol("price")
      .setOutputCol("priceNew")
      .fit(df)
    val countryIndexer = new StringIndexer()
      .setInputCol("country")
      .setOutputCol("CountryNew")
      .fit(df)
      val pointsIndexer = new StringIndexer()
      .setInputCol("points")
      .setOutputCol("pointsNew")
      .fit(df)
     var finaldf1 = countryIndexer.transform(df) 
     finaldf1 = princeIndexer.transform(finaldf1) 
     finaldf1 = pointsIndexer.transform(finaldf1) 
     finaldf1 = finaldf1
     .drop("country")
     .drop("PR1")
     .drop("PR2")
     .drop("PR3")
     .drop("PR4")
     //.drop("price").drop("points")
     finaldf1.show()
    //We'll split the set into training and test data
    val Array(trainingData, testData) = finaldf1.randomSplit(Array(0.8, 0.2))
    trainingData.show()
    val labelColumn = "priceNew"

    //We define two StringIndexers for the categorical variables

//    val countryIndexer = new StringIndexer()
//      .setInputCol("country")
//      .setOutputCol("countryIndex")
    //We define the assembler to collect the columns into a new column with a single vector - "features"
    val assembler = new VectorAssembler()
      .setInputCols(Array("pointsNew", "CountryNew"))
      .setOutputCol("features")

    //For the regression we'll use the Gradient-boosted tree estimator
    val gbt = new GBTRegressor()
      .setLabelCol(labelColumn)
      .setFeaturesCol("features")
      .setPredictionCol("Predicted " + labelColumn)
      .setMaxIter(50)

    //We define the Array with the stages of the pipeline
    val stages = Array(
      countryIndexer,
      assembler,
      gbt)

    //Construct the pipeline
    val pipeline = new Pipeline().setStages(stages)
    trainingData.show()
    //We fit our DataFrame into the pipeline to generate a model
    val model = pipeline.fit(trainingData)

    //We'll make predictions using the model and the test data
    val predictions = model.transform(testData)
    //This will evaluate the error/deviation of the regression using the Root Mean Squared deviation
    val evaluator = new RegressionEvaluator()
      .setLabelCol(labelColumn)
      .setPredictionCol("Predicted " + labelColumn)
      .setMetricName("rmse")
        val rmse = evaluator.evaluate(predictions)
       println("Root Mean Squared Error (RMSE) on test data = " + rmse)
     predictions.show()
     println(evaluator)
    //We compute the error using the evaluator
    val error = evaluator.evaluate(predictions)
    println(error)

  }
}