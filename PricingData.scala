package com.pricing

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import com.workflow.cx.CXTransformationBase

object PricingData extends CXTransformationBase {

  def main(arg: Array[String]) {
//    System.setProperty("hadoop.home.dir", "/etc/hadoop/conf");
    System.setProperty("hadoop.home.dir", "C:\\winutils");
    val isServer = false;
    var logger = Logger.getLogger(this.getClass())
    val jobName = "Pricingbase"
    var conf: SparkConf = null
    var csvInPath = ""
    var outputfileLoc = ""
    if (isServer == true) {
      conf = new SparkConf().setAppName(jobName).set("spark.driver.allowMultipleContexts", "true").set("spark.driver.maxResultSize", "5g")
      csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/CX_PQ_Milestone_PP.csv"
      outputfileLoc = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/out/metrics/"
    } else {
      conf = new SparkConf().setMaster("local[10]").setAppName(jobName).set("spark.executor.memory", "1g")
    }
    val sc = new SparkContext(conf)
    val sqlContext: SQLContext = new SQLContext(sc)

    def getpriceNotEquals(): DataFrame = {
      csvInPath = "C://Users//nookabh//Desktop//json//pricefiles//priceNotEquals.csv"
//      csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/out/priceNotEquals.csv"
      var rawTextRDD = sc.textFile(csvInPath)
      rawTextRDD = rawTextRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      val quoteSchemaFields = "cntry_code,PR4,ACCOUNT_NASP_NAME__C,count(SITE_ID),countDistinct(QTE_ID),min(Total_Price),avg(Total_Price),max(Total_Price),first(Total_Price)".split(",")
        .map(fieldName => StructField(fieldName, StringType, nullable = true))
      val quoteSchema = StructType(quoteSchemaFields)
      val rowRDD = rawTextRDD.map(_.split(",")).map(p => {
        if (p.length == 9) {
          val row: Row = Row.fromSeq(p.toSeq)
          row
        } else {
          Row("", "", "", "", "", "", "", "", "")
        }
      })
      // Apply the schema to the RDD.
      val base_df = sqlContext.createDataFrame(rowRDD, quoteSchema)
      base_df
    }
    
     def getpriceEquals(): DataFrame = {
      csvInPath = "C://Users//nookabh//Desktop//json//pricefiles//priceEquals.csv"
      var rawTextRDD = sc.textFile(csvInPath)
      rawTextRDD = rawTextRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      val quoteSchemaFields = "cntry_code,PR4,ACCOUNT_NASP_NAME__C,count(SITE_ID),countDistinct(QTE_ID),min(Total_Price),avg(Total_Price),max(Total_Price),first(Total_Price)".split(",")
        .map(fieldName => StructField(fieldName, StringType, nullable = true))
      val quoteSchema = StructType(quoteSchemaFields)
      val rowRDD = rawTextRDD.map(_.split(",")).map(p => {
        if (p.length == 9) {
          val row: Row = Row.fromSeq(p.toSeq)
          row
        } else {
          Row("", "", "", "", "", "", "", "", "")
        }
      })
      // Apply the schema to the RDD.
      val base_df = sqlContext.createDataFrame(rowRDD, quoteSchema)
      base_df
    }

    val pricenotequals = getpriceNotEquals()
    pricenotequals.sort("PR4","cntry_code")
    //.filter(pricenotequals("PR4")==="Application Aware Networking")
//    .filter(pricenotequals("cntry_code")==="AE")
    .show()

    val priceequals = getpriceEquals()
    priceequals
    //.sort("PR4","cntry_code")
    //.filter(priceequals("PR4")==="Application Aware Networking")
//    .filter(priceequals("cntry_code")==="AE")
    .show()
    
  }
}