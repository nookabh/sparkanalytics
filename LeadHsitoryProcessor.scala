package com.workflow.lead

import org.apache.spark.sql.types.StringType
import org.joda.time.format.DateTimeFormat
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructField
import org.apache.log4j.Logger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf


object LeadHsitoryProcessor {
  def main(arg: Array[String]) {
   //val base = readFileWithOutScheme()
   System.setProperty("hadoop.home.dir", "/opt/mapr/hadoop/hadoop-2.5.1/etc/hadoop");
    val isServer = false;
    var logger = Logger.getLogger(this.getClass())
    val jobName = "optyhistory"
    var conf: SparkConf = null
    var matrixNeme = DateTimeFormat.forPattern("YYYY_MM_dd").print(System.currentTimeMillis)
    var csvLeadInPath = "/Users/v494907/Downloads/json/leadData/LEAD_HISTORY.csv"
    var outputfileLoc = "/Users/v494907/Downloads/json/ProductTranformer/" + matrixNeme
    if (isServer == true) {
      conf = new SparkConf().setAppName(jobName)
      csvLeadInPath = "/mapr/my.cluster.com/workflow/PQ CX Dashboard.csv"
      outputfileLoc = "/mapr/my.cluster.com/workflow/out/" + matrixNeme
    } else {
      conf = new SparkConf().setMaster("local[2]").setAppName(jobName)
    }
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val leadSchemaFields = "CreatedById,CreatedDate,Field,Id,IsDeleted,LeadId,NewValue,OldValue".split(",")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    var base_df_lead = sqlContext.read.format("com.databricks.spark.csv").schema(StructType(leadSchemaFields)).option("header", "true").load(csvLeadInPath);
    base_df_lead = base_df_lead.filter(col("Field") === "Status" ).groupBy(col("LeadId")).agg(col("NewValue"))
    base_df_lead.show(200)
    
    //LEAD_ID,TIMESPENDINSTAGE0,TIMESPENTINSTAGE1
    //123,1,2
    
    //LEAD, NEW STAGE,WHEN DID IT ENGER THE STAGE
    //123,stage0,jan1
    //123,stage1,jan2
    //123,stage2,jan4
  }
}