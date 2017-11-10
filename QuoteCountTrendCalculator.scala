package com.workflow.trending

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
import org.apache.spark.sql.functions.udf
import com.workflow.rt.UtilityClass
import java.sql.Date
import scala.collection.mutable.ListBuffer
import org.apache.spark.storage.StorageLevel
import java.util.Calendar
import java.text.SimpleDateFormat
import org.apache.spark.sql.Row;
// Import Spark SQL data types
import org.apache.spark.sql.types.{ StructType, StructField, StringType };

/*Running instrucitons
 * 
 * cd /biginsights/iop/4.2.0.0/spark
 * export HADOOP_CONF_DIR=/etc/hadoop/4.2.0.0/0
 * ./bin/spark-submit --class com.workflow.trending.QuoteCountTrendCalculator --master yarn --deploy-mode cluster /tmp/spark0918.jar
 * 
 *
 */

object QuoteCountTrendCalculator {
  
   /* @author girish
   * 
   */
  
  def main(arg: Array[String]) {

    System.setProperty("hadoop.home.dir", "/opt/mapr/hadoop/hadoop-2.5.1/etc/hadoop");
    val isServer = true;
    var logger = Logger.getLogger(this.getClass())
    val jobName = "Quote CountWeekly Trend"
    var conf: SparkConf = null
    val now = Calendar.getInstance().getTime()
    val minuteFormat = new SimpleDateFormat("yyyy-MM-dd")
    val timestamp = minuteFormat.format(now)
    var matrixNeme = DateTimeFormat.forPattern("YYYY_MM_dd").print(System.currentTimeMillis)
    //var csvInPath = "/Users/v494907/Downloads/CX_PQ.csv"
    var csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/PQ_CX_09202017.csv"
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
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val schema = StructType(
      StructField("oppId", StringType, true) ::
        StructField("oppId", IntegerType, false) :: Nil)
    val df = sqlContext.createDataFrame(sc.emptyRDD[Row], schema)

    val parseToDate = udf(UtilityClass.dateStrToDateTime _)
    val comcatStringUDF = udf((a: String, b: String) => a + "-" + b)
//    val dateStrToWeekendDate = udf(UtilityClass.dateStrToWeekendDate _)
    var sundays = ListBuffer[Date]()
    UtilityClass.allSundaysOfAYear(2016).map(f => sundays += f)
    UtilityClass.allSundaysOfAYear(2017).map(f => sundays += f)
    
    val quoteSchemaFields = "Quote_id,Quote_version,PP_Quote_Type,base_quote_id,linked_quote_create_date,Oppty_create_Date,Long_oppty_id,short_oppty_id,milestone_date,MILESTONE_ID".split(",")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val quoteSchema = StructType(quoteSchemaFields)

    import sqlContext.implicits._
    var i:Int =0 ;
    val rawTextRDD = sc.textFile(csvInPath)
    val rowRDD = rawTextRDD.map(_.split("\\|")).map(p =>{
      if(p.length == 10){
        val row:Row = Row(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9)) 
        row
      }else{
        Row("","","","","","","","","","")
      }
    })
    // Apply the schema to the RDD.
    val base_df = sqlContext.createDataFrame(rowRDD, quoteSchema)
    base_df.show()

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
    
     val base_df_formatted = base_df.filter(col("MILESTONE_ID") === "10001010")
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