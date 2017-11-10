package com.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import java.text.SimpleDateFormat
import com.google.gson.Gson
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag
import org.apache.spark.sql
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import java.util.Date
import java.sql.Date

object ParallelProcessing {
   def main(arg: Array[String]) {
      var logger = Logger.getLogger(this.getClass())
      val jobName = "OptyHistory"
      val conf = new SparkConf().setMaster("local[2]").setAppName(jobName)
    
      val sc = new SparkContext(conf)
      val test = "";
      logger.info("Parallel Processing analysis");
      logger.info("=> jobName \"" + jobName + "\"")
    
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    
    
      val base_df = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("c:/VES/REQS/CX_Dashboard/CX Dashboard sample data 08162017/CX Dashboard milestone parallel processing sample data 08162017.csv")
      
      val base_quote_df = base_df.filter(base_df("PP_Quote_Type") === "Base Quote")
      val grouping_df = base_quote_df.groupBy("Long_oppty_id").count() 
      val final_df = grouping_df.withColumn("isPP_Adopted", when(grouping_df("count") > 0  , true).otherwise(false))
      final_df.show(20)
        
   }
     
}