package com.workflow.pipeline.example
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.RandomForestRegressionModel
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{StringIndexer, VectorIndexer}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.mllib.linalg.Vectors




object RandomForestReg {
  def main(arg: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils");
    var jobName = "TransformationMaster"
    var conf: SparkConf = null
    conf = new SparkConf().setMaster("local[10]").setAppName(jobName).set("spark.executor.memory", "1g").set("spark.driver.allowMultipleContexts", "true")
    var csvInPath = ""
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // Load and parse the data file, converting it to a DataFrame.
    val data1 = sqlContext.read.format("libsvm").load("C:\\Users\\nookabh\\Desktop\\Jupyter_Notebook\\trainingdata\\sample_little_libsvm.txt")

    def getPricingDF(): DataFrame = {
      csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/CX_PQ_PROD_FEAT_COMP.csv"
      var rawTextRDD = sc.textFile(csvInPath)
      rawTextRDD = rawTextRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      val quoteSchemaFields = "QTE_ID|REVIS_ID|quote_created_date|SITE_ID|SOLN_INST_ID|PROD_INST_ID|FEAT_INST_ID|addr_id|cntry_code|addr_type_code|line_1_text|line_2_text|line_3_text|line_4_text|city_name|terr_code|postal_number|postal_ext_number|PROD_CODE|USEC_Name|mlstn_name|PR1|PR2|PR3|PR4|MRC_AMT|NRC_AMT".split("\\|")
        .map(fieldName => StructField(fieldName, StringType, nullable = true))
      val quoteSchema = StructType(quoteSchemaFields)
      val rowRDD = rawTextRDD.map(_.split("\\|")).map(p => {
        if (p.length == 27) {
          Row.fromSeq(p.toSeq)
        } else {
          Row("", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "")
        }
      })
      var pricing_df = sqlContext.createDataFrame(rowRDD, quoteSchema)
      pricing_df
    }
     var base_pricing_df = getPricingDF()
    base_pricing_df = base_pricing_df
//      .withColumn("Full_Quote_Id", comcatStringUDF(col("QTE_ID"), col("REVIS_ID")))
      .withColumn("Full_Quote_Id", col("QTE_ID"))
//    base_pricing_df.show(10)
    base_pricing_df = base_pricing_df
      .withColumn("Total_Price", base_pricing_df("MRC_AMT")+base_pricing_df("NRC_AMT"))
//      .drop("NASP_ID")
      .drop("GCH_ID")
      .drop("LGL_ENTY_CODE")
      .drop("SITE_NAME")
      .drop("addr_type_code")
      .drop("line_1_text")
      .drop("line_2_text")
      .drop("line_3_text")
      .drop("line_4_text")
      .drop("terr_code")
      .drop("postal_ext_number")
      .drop("PROD_Name")
      .drop("FEAT_CODE")
      .drop("feature_name")
      .drop("mlstn_name")
      .drop("postal_number")
      .drop("quote_created_date")
//      .drop("SITE_ID")
      .drop("SOLN_INST_ID")
      .drop("PROD_INST_ID")
      .drop("FEAT_INST_ID")
      .drop("addr_id")
      .drop("postal_number")
    //.orderBy("QTE_ID")
   val data = base_pricing_df.select("Full_Quote_Id", "SITE_ID", "PR1", "PR2", "PR3", "PR4", "cntry_code", "Total_Price").limit(1000)
    data.show()
    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4 distinct values are treated as continuous.

//      val discretizer = new QuantileDiscretizer()
//        .setInputCol("Total_Price")
//        .setOutputCol("result")
//        .setNumBuckets(3)
//  
//      val result = discretizer.fit(data).transform(data)
//      result.show()

        val formula = new RFormula()
          .setFormula("Total_Price ~ cntry_code + PR4")
          .setLabelCol("label")
          .setFeaturesCol("features")
        var output = formula.fit(data).transform(data)
        output.show()
        output = output.select("label", "features")
//        val output = sqlContext.createDataFrame(Seq(
//      (1.0, Vectors.dense(0.0, 1.1, 0.1)),
//      (0.0, Vectors.dense(2.0, 1.0, -1.0)),
//      (0.0, Vectors.dense(2.0, 1.3, 1.0)),
//      (1.0, Vectors.dense(0.0, 1.2, -0.5))
//    )).toDF("label", "features")
//    output.show()
      val stages = new scala.collection.mutable.ArrayBuffer[PipelineStage]()
      val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(output)
      val featuresIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(10).fit(output)
      stages += featuresIndexer
      val tmp = featuresIndexer.transform(labelIndexer.transform(output))
      val rf = new RandomForestClassifier().setFeaturesCol(featuresIndexer.getOutputCol).setLabelCol(labelIndexer.getOutputCol)
      stages += rf
      val pipeline = new Pipeline().setStages(stages.toArray)
  
      // Fit the Pipeline
      val pipelineModel = pipeline.fit(tmp)
      val results = pipelineModel.transform(output)
      results.show
              
        val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(data)
      
//    val featureIndexer = new VectorIndexer()
//      .setInputCol("features")
//      .setOutputCol("indexedFeatures")
//      .setMaxCategories(4)
//      .fit(data)
//      featureIndexer.transform(data).show()
//
//     val assembler = new VectorAssembler()
//      .setInputCols(Array("PR4_Index" , "cntry_code_Index"))
//      .setOutputCol("features")
      /*
    // Split the data into training and test sets (30% held out for testing)
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    // Train a RandomForest model.
    val rf = new RandomForestRegressor()
      .setLabelCol("label")
      .setFeaturesCol("indexedFeatures")

    // Chain indexer and forest in a Pipeline
    val pipeline = new Pipeline()
      .setStages(Array(formula, rf))
    trainingData.show(5)
    println(trainingData.collectAsList().get(0).get(1))
    // Train model.  This also runs the indexer.
    val model = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)

    // Select example rows to display.
    predictions.select("prediction", "label", "features").show(5)

    // Select (prediction, true label) and compute test error
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)

    val rfModel = model.stages(1).asInstanceOf[RandomForestRegressionModel]
    println("Learned regression forest model:\n" + rfModel.toDebugString)*/
  }
}