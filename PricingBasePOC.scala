package com.pricing

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.joda.time.format.DateTimeFormat
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{
  StructType,
  StructField,
  StringType,
  IntegerType
}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.hive.HiveContext
import com.workflow.rt.UtilityClass
import java.sql.Date
import scala.collection.mutable.ListBuffer
import org.apache.spark.storage.StorageLevel
import java.util.Calendar
import java.text.SimpleDateFormat
import org.apache.spark.sql.Row;
// Import Spark SQL data types
import org.apache.spark.sql.types.{ StructType, StructField, StringType };
import com.workflow.cx.CXTransformationBase
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.feature.StringIndexer
import org.apache.log4j.Level
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.DecisionTreeRegressionModel

/*Running instrucitons
 * 
 * cd /biginsights/iop/4.2.0.0/spark
 * export HADOOP_CONF_DIR=/etc/hadoop/4.2.0.0/0
 * ./bin/spark-submit --class com.workflow.trending.QuoteCountTrendCalculator --master yarn --deploy-mode cluster /tmp/spark0918.jar
 * 
 *
 */

object PricingBasePOC extends CXTransformationBase {

  /* @author girish
   * 
   */

  def main(arg: Array[String]) {
//    System.setProperty("hadoop.home.dir", "/opt/mapr/hadoop/hadoop-2.5.1/etc/hadoop");
    System.setProperty("hadoop.home.dir", "C:\\winutils");
    val isServer = false;
    var logger = Logger.getLogger(this.getClass())
    val jobName = "Quote CountWeekly Trend"
    var conf: SparkConf = null
    var csvInPath = ""
    var outputfileLoc = ""
    if (isServer == true) {
      conf = new SparkConf().setAppName(jobName)
      csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/PQ_CX_09202017.csv"
      outputfileLoc = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/out/-QUOTE_COUNT_TREND"
    } else {
      conf = new SparkConf().setMaster("local[10]").setAppName(jobName).set("spark.executor.memory", "1g")
    }
    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //Logger.getRootLogger().setLevel(Level.OFF)
//    Logger.getLogger("org").setLevel(Level.OFF)
//    Logger.getLogger("akka").setLevel(Level.OFF)
    val sqlContext: SQLContext = new SQLContext(sc)

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
    
     def getFinalPricing(): DataFrame = {
      csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/CX_PQ_PROD_FEAT_COMP.csv"
      var rawTextRDD = sc.textFile(csvInPath)
      rawTextRDD = rawTextRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      val quoteSchemaFields = "QTE_ID|REVIS_ID|OPTY_ID|NASP_ID|GCH_ID|CLE_NAME|quote_created_date|SITE_ID|SOLN_INST_ID|PROD_INST_ID|FEAT_INST_ID|addr_id|cntry_code|addr_type_code|line_1_text|line_2_text|line_3_text|line_4_text|city_name|terr_code|postal_number|postal_ext_number|PROD_CODE|USEC_Name|mlstn_name|PR1|PR2|PR3|PR4|MRC_AMT|NRC_AMT".split("\\|")
        .map(fieldName => StructField(fieldName, StringType, nullable = true))
      val quoteSchema = StructType(quoteSchemaFields)
      val rowRDD = rawTextRDD.map(_.split("\\|")).map(p => {
        if (p.length == 31) {
          val row: Row = Row.fromSeq(p.toSeq)
          row
        } else {
          Row("", "", "", "", "", "", "", "", "", "","", "", "", "", "", "", "", "", "", "","", "", "", "", "", "", "", "", "", "", "")
        }
      })
      // Apply the schema to the RDD.
      val base_df = sqlContext.createDataFrame(rowRDD, quoteSchema)
      base_df
    }
     
     var pricingdf = getFinalPricing().withColumn("Total_Price", col("MRC_AMT") + col("NRC_AMT"))
    pricingdf.show()

     var bdf = pricingdf.groupBy("cntry_code", "PR4", "NASP_ID").agg(count("SITE_ID"), countDistinct("QTE_ID"), min("Total_Price"), avg("Total_Price"), max("Total_Price"), first(col("Total_Price")))
    bdf = bdf.sort("cntry_code")
    val priceEquals = bdf.filter(col("min(Total_Price)") === col("max(Total_Price)"))
    priceEquals.coalesce(1).write.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("treatEmptyValuesAsNulls", "true")  // No effect.
        .save("C://Users//nookabh//Desktop//json//pricefiles//price//priceequals")
        
    val priceNotEquals = bdf.filter(col("min(Total_Price)") !== col("max(Total_Price)"))
    priceNotEquals.coalesce(1).write.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("treatEmptyValuesAsNulls", "true")  // No effect.
        .save("C://Users//nookabh//Desktop//json//pricefiles//price//pricenotequals")
    val head = "\"cntry_code\",\"PR4\", \"ACCOUNT_NASP_NAME__C\",\"count(SITE_ID)\",\"countDistinct(QTE_ID)\",\"min(Total_Price)\", \"avg(Total_Price)\", \"max(Total_Price)\", \"first(Total_Price)\"" + System.lineSeparator

    this.writeFile(priceEquals, "priceEquals", head)
    this.writeFile(priceNotEquals, "priceNotEquals", head)
    
//    val schema = StructType(
//      StructField("oppId", StringType, true) ::
//        StructField("oppId", IntegerType, false) :: Nil)
//    val df = sqlContext.createDataFrame(sc.emptyRDD[Row], schema)
//
//    val parseToDate = udf(UtilityClass.dateStrToDateTime _)
//    val parseToDateMS = udf(UtilityClass.dateStrToMilliSeconds _)
//    val comcatStringUDF = udf((a: String, b: String) => a + "-" + b)
//    //    val dateStrToWeekendDate = udf(UtilityClass.dateStrToWeekendDate _)
//
//    var base_pricing_df = getPricingDF()
//    base_pricing_df = base_pricing_df
//      .withColumn("Full_Quote_Id", comcatStringUDF(col("QTE_ID"), col("REVIS_ID")))//.limit(1000)
//    base_pricing_df.show()
//    base_pricing_df = base_pricing_df
//      .withColumn("Total_Price", base_pricing_df("MRC_AMT")+base_pricing_df("NRC_AMT"))
//      .drop("NASP_ID")
//      .drop("GCH_ID")
//      .drop("LGL_ENTY_CODE")
//      .drop("SITE_NAME")
//      .drop("addr_type_code")
//      .drop("line_1_text")
//      .drop("line_2_text")
//      .drop("line_3_text")
//      .drop("line_4_text")
//      .drop("terr_code")
//      .drop("postal_ext_number")
//      .drop("PROD_Name")
//      .drop("FEAT_CODE")
//      .drop("feature_name")
//      .drop("mlstn_name")
//      //.drop("postal_number")
//      .drop("quote_created_date")
//      //.drop("SITE_ID")
//      .drop("SOLN_INST_ID")
//      .drop("PROD_INST_ID")
//      .drop("FEAT_INST_ID")
//      //.drop("addr_id")
//    //.orderBy("QTE_ID")
//    //base_pricing_df.show()
//      
//    base_pricing_df = base_pricing_df.groupBy("QTE_ID","SITE_ID","PR4","addr_id","cntry_code").agg(sum("Total_Price") as "Total_Price")
//   
//    val indexerPR4 = new StringIndexer()
//      .setInputCol("PR4")
//      .setOutputCol("PR4_Index")
//      .fit(base_pricing_df)
//    val indexedPR4 = indexerPR4.transform(base_pricing_df)
    /*val indexerPR3 = new StringIndexer()
      .setInputCol("PR3")
      .setOutputCol("PR3_Index")
      .fit(indexedPR4)
    val indexedPR3 = indexerPR3.transform(indexedPR4)
    val indexerPR2 = new StringIndexer()
      .setInputCol("PR2")
      .setOutputCol("PR2_Index")
      .fit(indexedPR3)
    val indexedPR2 = indexerPR2.transform(indexedPR3)
    val indexerPR1 = new StringIndexer()
      .setInputCol("PR1")
      .setOutputCol("PR1_Index")
      .fit(indexedPR2)
    val indexedPR1 = indexerPR1.transform(indexedPR2)*/
//    val indexerCountry = new StringIndexer()
//      .setInputCol("cntry_code")
//      .setOutputCol("cntry_code_Index")
//      .fit(indexedPR4)
//   val indexedCountry = indexerCountry.transform(indexedPR4)
//   val indexerCity = new StringIndexer()
//      .setInputCol("addr_id")
//      .setOutputCol("postal_number_Index")
//      .fit(indexedCountry)
//    val indexedCity = indexerCity.transform(indexedCountry)
//    base_pricing_df = indexedCity.drop("PR1").drop("PR2").drop("PR3").drop("PR4").drop("cntry_code").drop("addr_id")
//    //base_pricing_df.show(100)
    
    
   /* val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    var outLoc = "C:/ac/TestData/scala/out/"
    var outputFile = new Path(outLoc + "Price_Indexed.csv"): Path
    if (hdfs.exists(outputFile)) {
        hdfs.delete(outputFile, true)
      }
    //base_pricing_df.show()
    base_pricing_df//.coalesce(1)
      .write.format("com.databricks.spark.csv")
      //.option("header", "true")
      .option("treatEmptyValuesAsNulls", "true") // No effect.
      .save("C:/ac/TestData/scala/out/Price_Indexed")

    
    FileUtil.copyMerge(hdfs, new Path(outLoc + "Price_Indexed/"), hdfs, new Path(outLoc + "/Price_Indexed.csv"), true, hadoopConfig, null)*/
    /*
    
    //Start Algorithm
    val Array(trainingData, testData) = base_pricing_df.randomSplit(Array(0.8, 0.2))
    
     val labelColumn = "Total_Price"
     
      val assembler = new VectorAssembler()
      .setInputCols(Array("PR4_Index","cntry_code_Index"))
      .setOutputCol("features")
      
      /*val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(base_pricing_df)*/
      
      val gbt = new GBTRegressor()
      .setLabelCol(labelColumn)
      .setFeaturesCol("features")
      .setPredictionCol("Predicted " + labelColumn)
      .setMaxIter(50)
      .setMaxBins(77)
      
      val stages = Array(
      indexerPR4,indexerCountry,
      assembler,
      gbt)
 val pipeline = new Pipeline().setStages(stages)
    trainingData.show()
     val model = pipeline.fit(trainingData)
 //We'll make predictions using the model and the test data
    val predictions = model.transform(testData)
    predictions.show()
    
    // Select (prediction, true label) and compute test error.
    val evaluator = new RegressionEvaluator()
      .setLabelCol(labelColumn)
      .setPredictionCol("Predicted " + labelColumn)
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)

    //val treeModel = model.stages(1).asInstanceOf[DecisionTreeRegressionModel]
    //println("Learned regression tree model:\n" + treeModel.toDebugString)
    
    
*/
  }
}