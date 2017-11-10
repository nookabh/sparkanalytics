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

/*Running instrucitons
 * 
 * cd /biginsights/iop/4.2.0.0/spark
 * export HADOOP_CONF_DIR=/etc/hadoop/4.2.0.0/0
 * ./bin/spark-submit --class com.workflow.trending.QuoteCountTrendCalculator --master yarn --deploy-mode cluster /tmp/spark0918.jar
 * 
 *
 */

object PricingBase extends CXTransformationBase{
  
   /* @author girish
   * 
   */
  
  
  def getSales_Directors(sc:SparkContext,sqlContext:SQLContext): DataFrame = {
      var csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/sales_directors.csv"
      val rawTextRDD = sc.textFile(csvInPath)
      val salesSchemaFields = "ID,OWNERID,ISDELETED,NAME,CREATEDDATE,CREATEDBYID,LASTMODIFIEDDATE,LASTMODIFIEDBYID,SYSTEMMODSTAMP,LASTACTIVITYDATE,LASTVIEWEDDATE,LASTREFERENCEDDATE,ALLNASP_DIR_NAME__C,VZ_ID__C,GVP__C,RVP__C,SVP__C,GVP_USER__C,RVP_USER__C,SVP_USER__C,ZVP_USER__C".split(",")
        .map(fieldName => StructField(fieldName, StringType, nullable = true))
      val salesdirSchema = StructType(salesSchemaFields)
      val rowRDD = rawTextRDD.map(_.split(",")).map(p => {
        var indexofRow:Integer = 0;
        val updatedRowBuffer = ListBuffer[String]()
        for(pvalue <- p){
          if(indexofRow != p.length -2 && indexofRow != p.length -1){
            updatedRowBuffer += pvalue
          }
          indexofRow += 1
        }
        updatedRowBuffer += p(p.length - 1) + " " + p(p.length - 2)
        val updatedRow = updatedRowBuffer.toList
        if (updatedRow.length == 21 || updatedRow(0) == "Id") {
          val row: Row = Row.fromSeq(updatedRow.toSeq)
          row
        } else {
          Row("", "", "", "", "","", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "")
        }
      })
      // Apply the schema to the RDD.
      val salesdirresults = sqlContext.createDataFrame(rowRDD, salesdirSchema)
      salesdirresults.show()
      salesdirresults
    }


 def getUsers(sc:SparkContext,sqlContext:SQLContext): DataFrame = {
//       var csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/view_cx_user.csv"
       var csvInPath = "C:/Word_Dir/Metric/view_cx_user.csv"
      val rawTextRDD = sc.textFile(csvInPath)
      val userSchemaFields = "ID,ENTERPRISE_ID__C,VZ_ID__C,EID_0__C,EID_10__C,EID_11__C,EID_12__C,EID_13__C,EID_14__C,EID_15__C,EID_1__C,EID_2__C,EID_3__C,EID_4__C,EID_5__C,EID_6__C,EID_7__C,EID_8__C,EID_9__C".split(",")
        .map(fieldName => StructField(fieldName, StringType, nullable = false))
      val userSchema = StructType(userSchemaFields)
      val rowRDD = rawTextRDD.map(_.split(",")).map(p => {
        if (p.length == 19) {
          val row: Row = Row(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14), p(15), p(16), p(17), p(18))
          row
        } else {
          Row("", "", "", "", "","", "", "", "", "", "", "", "", "", "", "", "", "", "")
        }
      })
      // Apply the schema to the RDD.
      val userresults = sqlContext.createDataFrame(rowRDD, userSchema)
      userresults.show()
      userresults
    }
  
  def main(arg: Array[String]) {

    System.setProperty("hadoop.home.dir", "/opt/mapr/hadoop/hadoop-2.5.1/etc/hadoop");
    val isServer = false;
    var logger = Logger.getLogger(this.getClass())
    val jobName = "Quote CountWeekly Trend"
    var conf: SparkConf = null
    val now = Calendar.getInstance().getTime()
    val minuteFormat = new SimpleDateFormat("yyyy-MM-dd")
    val timestamp = minuteFormat.format(now)
    var matrixNeme = DateTimeFormat.forPattern("YYYY_MM_dd").print(System.currentTimeMillis)
    //var csvInPath = "/Users/v494907/Downloads/CX_PQ.csv"
    
    var csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/CX_PQ_SITE_PROD_FEAT.csv"
    var parequeFilePath = "/Users/v494907/Downloads/json/CX_PQ_Parquet" + System.currentTimeMillis
    var outputfileLoc = "/Users/v494907/Downloads/json/" + matrixNeme + "/" + System.currentTimeMillis + "/" + matrixNeme
    if (isServer == true) {
      conf = new SparkConf().setAppName(jobName)
      csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/PQ_CX_09202017.csv"
      outputfileLoc = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/out/" + timestamp + "-" + System.currentTimeMillis + "-QUOTE_COUNT_TREND"
    } else {
      conf = new SparkConf().setMaster("local[10]").setAppName(jobName).set("spark.executor.memory", "1g")
    }
     val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
     
     
     val sqlContext: SQLContext = new SQLContext(sc) 

     
     getSales_Directors(sc,sqlContext)
     
     def getBaseDF(): DataFrame = {
      csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/CX_PQ_Milestone_PP.csv"
      val rawTextRDD = sc.textFile(csvInPath)
      val quoteSchemaFields = "Quote_id|Quote_version|PP_Quote_Type|linked_quote_create_date|base_quote_id|Oppty_create_Date|Long_oppty_id|short_oppty_id|OPPORTUNITY_NAME|ACCOUNT_NAME|milestone_date|MILESTONE_ID|MILESTONE_DESC".split("\\|")
        .map(fieldName => StructField(fieldName, StringType, nullable = true))
      val quoteSchema = StructType(quoteSchemaFields)
      val rowRDD = rawTextRDD.map(_.split("\\|")).map(p => {
        if (p.length == 13) {
          val row: Row = Row(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12))
          row
        } else {
          Row("", "", "", "", "", "", "", "", "", "", "", "", "")
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
    base_pricing_df = base_pricing_df
                      .withColumn("Full_Quote_Id", comcatStringUDF(col("QTE_ID"), col("REVIS_ID")))
                      .filter(col("Full_Quote_Id") === "193908434-1")
    base_pricing_df.show(1000)                
    base_pricing_df =  base_pricing_df
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
    base_pricing_df.show()
     val indexer2 = new StringIndexer()
      .setInputCol("PROD_CODE")
      .setOutputCol("PROD_CODE_Index")
      .fit(base_pricing_df)  
    val indexed2 = indexer2.transform(base_pricing_df)
    
    val encoder = new OneHotEncoder()
      .setInputCol("PROD_CODE_Index")
      .setOutputCol("PROD_CODE_VEC")
    val encoded = encoder.transform(indexed2)
    encoded.show();
    
    //base_pricing_df = base_pricing_df.groupBy($"Full_Quote_Id").agg(collect_list($"PROD_CODE") as "quote_products", collect_list($"cntry_code") as "quote_countries", sum("MRC_AMT") as "quote_mrc", sum("NRC_AMT") as "quote_nrc")
    
    
    base_pricing_df.show()
    var base_df = getBaseDF()
    base_df = base_df.withColumn("Full_Quote_Id", comcatStringUDF(col("Quote_id"), col("Quote_version")))
                      .withColumn("milestone_date_dt_ms", parseToDateMS(col("milestone_date")))
                      //.withColumn("FULL_QUOTEID_LATESTE_MILESTORE_DATE", comcatStringUDF(col("Full_Quote_Id"), col("milestone_date_dt_ms")))
    base_df.show()
    var base_df_min = base_df
                      .groupBy('Full_Quote_Id)
                      .agg(min(struct('milestone_date_dt_ms,'MILESTONE_ID)) as 'milestoreIdandTimeStruct)
                      .select($"Full_Quote_Id", $"milestoreIdandTimeStruct.*")
                      
                      
                      
   base_df_min.show()
   var base_df_max = base_df
                      .groupBy("Full_Quote_Id")
                      .agg(max("milestone_date_dt_ms") as "Quote_Latest_Status_Date_MS")
              
    base_df_max.show()
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