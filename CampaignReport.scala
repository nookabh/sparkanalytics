package com.workflow.lead

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import java.text.SimpleDateFormat
import org.joda.time.format.DateTimeFormat
import scala.reflect.api.materializeTypeTag
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import scala.collection.mutable.ListBuffer



object CampaignReport {

    def getCampDF(sqlContext:SQLContext,sc:SparkContext ): DataFrame = {
    var csvInPath = "C:/SparkAnalytics/Marketing_Project/in/CampaignDetails.csv"
    var rawTextRDD = sc.textFile(csvInPath)
    rawTextRDD = rawTextRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    val SchemaFields = "createdAt,active,workspaceName,id,type,programId,updatedAt".split(",")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val Schema = StructType(SchemaFields)
    val rowRDD = rawTextRDD.map(_.split(",")).map(p => {
       var pCleans = ListBuffer[String]()
       for (a <- p){
         pCleans += a.replaceAll("'", "").replaceAll("\"","")
       }
      var pCleansList:List[String] = pCleans.toList;
      if (p.length <= 7) {
        val row: Row = Row(pCleansList(0), pCleansList(1), pCleansList(2), pCleansList(3), pCleansList(4), pCleansList(5), pCleansList(6))
        row
      } else {
        Row("", "", "", "", "", "", "")
      }
    })
    // Apply the schema to the RDD.
    val base_df = sqlContext.createDataFrame(rowRDD,Schema)
    //base_df.show()
    base_df
  } 
    
  
  def main(arg: Array[String]) {
  	 	val isServer = false;
		if (isServer == true) {
			System.setProperty("hadoop.home.dir", "/opt/mapr/hadoop/hadoop-2.5.1/etc/hadoop"); //For server run 
		} else {
			System.setProperty("hadoop.home.dir", "C:\\winutils");  //For local run    
		}
		
			  val jobName = "CampaignReport"
				var conf = new SparkConf().setMaster("local[2]").setAppName(jobName)
				
				val sc = new SparkContext(conf)
				val sqlContext = new SQLContext(sc)	
        import sqlContext.implicits._
        
        var csvInPath_camp = "C:/SparkAnalytics/Marketing_Project/in/CampaignDetails 2017-10-12.csv"
        var base_df_camp = getCampDF(sqlContext,sc);
/*        var base_df_camp = sqlContext.read.format("com.databricks.spark.csv")
				.option("header", "true") // Use first line of all files as header
				.option("inferSchema", "true") // Automatically infer data types
				//.option("delimiter", "|")
				.load(csvInPath_camp)*/
				base_df_camp.show()
  }
}