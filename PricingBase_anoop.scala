package com.examples

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
//import org.apache.spark.sql.types.{ StructType, StructField, StringType };
import com.workflow.cx.CXTransformationBase
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Column
import org.apache.log4j.Level
import org.apache.log4j.Logger
/*Running instrucitons
 * 
 * cd /biginsights/iop/4.2.0.0/spark
 * export HADOOP_CONF_DIR=/etc/hadoop/4.2.0.0/0
 * ./bin/spark-submit --class com.workflow.trending.QuoteCountTrendCalculator --master yarn --deploy-mode cluster /tmp/spark0918.jar
 * 
 *
 */

object PricingBase_anoop extends CXTransformationBase{
  
   /* @author girish
   * 
   */
  
  def main(arg: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils");
    //System.setProperty("hadoop.home.dir", "/opt/mapr/hadoop/hadoop-2.5.1/etc/hadoop");
    val isServer = false;
    var logger = Logger.getLogger(this.getClass())
    val jobName = "Quote CountWeekly Trend"
    var conf: SparkConf = null
    val now = Calendar.getInstance().getTime()
    val minuteFormat = new SimpleDateFormat("yyyy-MM-dd")
    val timestamp = minuteFormat.format(now)
    var matrixNeme = DateTimeFormat.forPattern("YYYY_MM_dd").print(System.currentTimeMillis)
    //var csvInPath = "/Users/v494907/Downloads/CX_PQ.csv"
    
    var csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/Shrink1k_CX_PQ_SITE.csv"
    var parequeFilePath = "/Users/v494907/Downloads/json/CX_PQ_Parquet" + System.currentTimeMillis
    var outputfileLoc = "C:/ac/TestData/scala/out/metrics/"
    if (isServer == true) {
      conf = new SparkConf().setAppName(jobName)
      csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/PQ_CX_09202017.csv"
      outputfileLoc = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/out/" + timestamp + "-" + System.currentTimeMillis + "-QUOTE_COUNT_TREND"
    } else {
      conf = new SparkConf().setMaster("local[10]").setAppName(jobName).set("spark.executor.memory", "1g")
    }
     val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
     val sqlContext: SQLContext = new HiveContext(sc) 
 //Logger.getRootLogger().setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

      def getBaseDF(): DataFrame = {
      csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/Shrink_PQ_Milestone.csv"
      val rawTextRDD = sc.textFile(csvInPath)
      val quoteSchemaFields = "Quote_id|Quote_version|Quote_created_date|PP_Quote_Type|linked_quote_create_date|base_quote_id|Oppty_create_Date|Long_oppty_id|short_oppty_id|OPPORTUNITY_NAME|ACCOUNT_NAME|milestone_date|MILESTONE_ID|MILESTONE_DESC".split("\\|")
        .map(fieldName => StructField(fieldName, StringType, nullable = true))
      val quoteSchema = StructType(quoteSchemaFields)
      val rowRDD = rawTextRDD.map(_.split("\\|")).map(p => {
        if (p.length == 14) {
          val row: Row = Row(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13))
          row
        } else {
          Row("", "", "", "", "", "", "", "", "", "", "", "", "", "")
        }
      })
      // Apply the schema to the RDD.
      val base_df = sqlContext.createDataFrame(rowRDD, quoteSchema)
      base_df.show()
      base_df
    }
   
    val schema = StructType(
      StructField("oppId", StringType, true) ::
        StructField("oppId", IntegerType, false) :: Nil)
    val df = sqlContext.createDataFrame(sc.emptyRDD[Row], schema)

    val parseToDate = udf(UtilityClass.dateStrToDateTime _)
    val parseToDateMS = udf(UtilityClass.dateStrToMilliSeconds _)
    val comcatStringUDF = udf((a: String, b: String) => a + "-" + b)
//    val dateStrToWeekendDate = udf(UtilityClass.dateStrToWeekendDate _)
    var sundays = ListBuffer[Date]()
    UtilityClass.allSundaysOfAYear(2016).map(f => sundays += f)
    UtilityClass.allSundaysOfAYear(2017).map(f => sundays += f)
    
    val quoteSchemaFields = "QTE_ID,REVIS_ID,OPTY_ID,NASP_ID,GCH_ID,LGL_ENTY_CODE,SITE_NAME,addr_type_code,cntry_code,line_1_text,line_2_text,line_3_text,line_4_text,city_name,terr_code,postal_number,postal_ext_number,PROD_CODE,Prod_Name,FEAT_CODE,feature_name,MRC_AMT,NRC_AMT".split(",")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val quoteSchema = StructType(quoteSchemaFields)

    import sqlContext.implicits._
    var i:Int =0 ;
    val rawTextRDD = sc.textFile(csvInPath)
    val rowRDD = rawTextRDD.map(_.split("\\|")).map(p =>{
      if(p.length == 23){
        Row.fromSeq(p.toSeq)
      }else{
        Row("","","","","","","","","","","","","","","","","","","","","","","")
      }
    })
    
   
    
    // Apply the schema to the RDD.
    var base_pricing_df = sqlContext.createDataFrame(rowRDD, quoteSchema)
    base_pricing_df =  base_pricing_df.withColumn("Full_Quote_Id", comcatStringUDF(col("QTE_ID"), col("REVIS_ID")))
                      .drop("NASP_ID")
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
    //.orderBy("QTE_ID")
    //base_pricing_df.show()
    //val base_pricing_df1 = base_pricing_df.groupBy($"Full_Quote_Id",$"PROD_CODE").agg(collect_set($"PROD_CODE") as "quote_products", collect_list($"cntry_code") as "quote_countries",sum("MRC_AMT") as "quote_mrc", sum("NRC_AMT") as "quote_nrc")
    //base_pricing_df1.show()
    //base_pricing_df = base_pricing_df.groupBy($"Full_Quote_Id",$"PROD_CODE").agg(collect_set($"PROD_CODE") as "quote_products", collect_list($"cntry_code") as "quote_countries", sum("MRC_AMT") as "quote_mrc", sum("NRC_AMT") as "quote_nrc")
    //                  .withColumn("SitesCount",size($"quote_countries"))
    base_pricing_df = base_pricing_df.groupBy($"PROD_CODE").agg(sum("MRC_AMT") as "sum_quote_mrc",min("MRC_AMT") as "min_quote_mrc",max("MRC_AMT") as "max_quote_mrc", sum("NRC_AMT") as "sum_quote_nrc",min("NRC_AMT") as "min_quote_nrc",max("NRC_AMT") as "max_quote_nrc")
                     
    base_pricing_df.where((base_pricing_df("max_quote_mrc") - base_pricing_df("min_quote_mrc") > 3) and (base_pricing_df("max_quote_nrc") - base_pricing_df("min_quote_nrc") > 3)).show()
    
    var base_df = getBaseDF()
    base_df = base_df.withColumn("Full_Quote_Id", comcatStringUDF(col("Quote_id"), col("Quote_version")))
                      .withColumn("milestone_date_dt_ms", parseToDateMS(col("milestone_date")))
                      
                      //.where(base_df("Quote_id") !== null)
                      //.withColumn("FULL_QUOTEID_LATESTE_MILESTORE_DATE", comcatStringUDF(col("Full_Quote_Id"), col("milestone_date_dt_ms")))
    //base_df.show()
    var base_df_min = base_df
                      .groupBy('Full_Quote_Id)
                      .agg(min(struct('milestone_date_dt_ms,'MILESTONE_ID)) as 'milestoreIdandTimeStruct)
                      .select($"Full_Quote_Id", $"milestoreIdandTimeStruct.milestone_date_dt_ms" as "MinMilestoneMS")
   //base_df_min.show()
   var base_df_max = base_df
                      .groupBy('Full_Quote_Id)
                      .agg(max(struct('milestone_date_dt_ms,'MILESTONE_ID)) as 'milestoreIdandTimeStruct)
                      .select($"Full_Quote_Id",$"milestoreIdandTimeStruct.MILESTONE_ID" as "MaxMilestone" )
              
    base_df_max.show()
    /*val joinDF = base_pricing_df.join(base_df_max, base_df_max("Full_Quote_Id") === base_pricing_df("Full_Quote_Id"))
    
     val joinDF2 = joinDF.join(base_df_min, base_df_min("Full_Quote_Id") === base_pricing_df("Full_Quote_Id"))
     joinDF2.show()*/
    println("done");

//    val base_df_formatted = base_df.filter(col("MILESTONE_ID") === "10001010")
//      .withColumn("milestone_date_dt", parseToDate(col("milestone_date")))
//      .withColumn("milestone_date_week", dateStrToWeekendDate(col("milestone_date")))
//      .withColumn("Full_Quote_Id", comcatStringUDF(col("Quote_id"), col("Quote_version")))
//      .orderBy(col("milestone_date_dt"))
//      .drop("milestone_date_dt")
//      .drop("Long_oppty_id")
//      .drop("PP_Quote_Type")
//      .drop("Quote_version")
//      .drop("linked_quote_create_date")
//      .drop("base_quote_id")
//      .orderBy(col("milestone_date_week"))
    
     val base_df_formatted = base_pricing_df.filter(col("MILESTONE_ID") === "10001010")
      .withColumn("milestone_date_dt", parseToDate(col("milestone_date")))
      .withColumn("milestone_date_week", parseToDate(col("milestone_date")))
      .withColumn("Full_Quote_Id", comcatStringUDF(col("Quote_id"), col("Quote_version")))
      .orderBy(col("milestone_date_dt"))
      .drop("milestone_date_dt")
      .drop("Long_oppty_id")
      .drop("PP_Quote_Type")
      .drop("Quote_version")
      .drop("linked_quote_create_date")
      .drop("base_quote_id")
      .orderBy(col("milestone_date_week"))

    base_df_formatted.registerTempTable("base_df_grouped")
    val base_df_grouped = sqlContext.sql("SELECT short_oppty_id,milestone_date_week, count(Full_Quote_Id) as Quote_ids FROM base_df_grouped GROUP BY milestone_date_week,short_oppty_id")
    val base_df_grouped_per = base_df_grouped.persist(StorageLevel.MEMORY_AND_DISK)
    val schemaFinal = StructType(
      StructField("short_oppty_id", StringType, false) ::
        StructField("Quote_ids", IntegerType, false) ::
        StructField("milestone_date_week", StringType, true) :: Nil)
    var finalDF = sqlContext.createDataFrame(sc.emptyRDD[Row], schemaFinal);



    for (a <- sundays.toList) {
      var tempDF = base_df_grouped.filter(base_df_grouped("milestone_date_week").leq(a))

      val tempDF2 = tempDF.drop("milestone_date_week")

      tempDF2.registerTempTable("tempDF2");
      var results = sqlContext.sql("SELECT short_oppty_id, sum(Quote_ids) as Quote_ids FROM tempDF2 GROUP BY short_oppty_id")

      results = results.withColumn("milestone_date_week", lit(a))
      finalDF = finalDF.unionAll(results)
      logger.info("success::" + a)
    }

    //finalDF.show()
    finalDF.coalesce(1).write.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("treatEmptyValuesAsNulls", "true") // No effect.
      .save(outputfileLoc)
  }
  def returnSame(a: Date): Date = {
    return a
  }
}