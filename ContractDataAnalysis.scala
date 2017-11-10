package com.workflow.batch.trails

import java.text.SimpleDateFormat

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.min
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.joda.time.format.DateTimeFormat

object ContractDataAnalysis {

  def transpose(hc: SQLContext, df: DataFrame, compositeId: List[String], key: String, value: String) = {

    val distinctCols = df.select(key).distinct.map { r => r(0) }.collect().toList

    val rdd = df.map { row =>
      (compositeId.collect { case id => row.getAs(id).asInstanceOf[Any] },
        scala.collection.mutable.Map(row.getAs(key).asInstanceOf[Any] -> row.getAs(value).asInstanceOf[Any]))
    }
    val pairRdd = rdd.reduceByKey(_ ++ _)
    val rowRdd = pairRdd.map(r => dynamicRow(r, distinctCols))
    hc.createDataFrame(rowRdd, getSchema(df.schema, compositeId, (key, distinctCols)))

  }

  private def dynamicRow(r: (List[Any], scala.collection.mutable.Map[Any, Any]), colNames: List[Any]) = {
    val cols = colNames.collect { case col => r._2.getOrElse(col.toString(), null) }
    val array = r._1 ++ cols
    Row(array: _*)
  }

  private def getSchema(srcSchema: StructType, idCols: List[String], distinctCols: (String, List[Any])): StructType = {
    val idSchema = idCols.map { idCol => srcSchema.apply(idCol) }
    val colSchema = srcSchema.apply(distinctCols._1)
    val colsSchema = distinctCols._2.map { col => StructField(col.asInstanceOf[String], colSchema.dataType, colSchema.nullable) }
    StructType(idSchema ++ colsSchema)
  }

  def convertStrToDate(a: String): Long = {

    val inputFormat = new SimpleDateFormat("dd-MMM-yy")
    if (a == null) {
      a
    }
    val sdate = inputFormat.parse(a.split(" ")(0));
    sdate.getTime
  }
  def convertSFDCDTStrToDate(a: String): Long = {
    val inputFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    if (a == null) {
      a
    }
    val sdate = inputFormat.parse(a);
    sdate.getTime
  }
  def convertModificaitonToFinalStatus(a: String): String = {
    if (a.indexOf("Associated") < 0)
      a.split(" ")(a.split(" ").size - 1)
    else
      "Creation"
  }

  def main(arg: Array[String]) {
    var logger = Logger.getLogger(this.getClass())
    val jobName = "Contract Data calculation"
    val isServer = true
    var conf: SparkConf = null
    var optyHistoryPath: String = null
    var contractHistoryPath: String = null
    var outputfileLoc: String = null
    if (isServer == true) {
      outputfileLoc = "/mapr/my.cluster.com/workflow/contractOutput"
      contractHistoryPath = "/mapr/my.cluster.com/workflow/cx_export_contract.csv"
      optyHistoryPath = "/mapr/my.cluster.com/workflow/OptyData.csv"
      conf = new SparkConf().setAppName(jobName)
    } else {
      conf = new SparkConf().setMaster("local[2]").setAppName(jobName)
      optyHistoryPath = "/Users/v494907/Documents/hack/files/OptyData.csv"
      contractHistoryPath = "/Users/v494907/Documents/hack/files/cx_export_contract.csv"
      outputfileLoc = "/Users/v494907/Documents/hack/files/contractOutput"
    }

    val sc = new SparkContext(conf)
    try {
      val sqlContext = new SQLContext(sc);
      logger.info("Opty History analysis");
      logger.info("=> jobName \"" + jobName + "\"")

      val optyDataFields = "Amount,CloseDate,CreatedById,CreatedDate,ExpectedRevenue,ForecastCategory,Id,IsDeleted,OpportunityId,Probability,StageName,SystemModstamp".split(",")
        .map(fieldName => StructField(fieldName, StringType, nullable = true))
      val optySchema = StructType(optyDataFields)

      val contractFields = "QUOTE_ID,QUOTE_VERSION_ID,DOCID,EVENT,MODIFICATION_DATE,MODIFICATION".split(",")
        .map(fieldName => StructField(fieldName, StringType, nullable = true))
      val contractSchema = StructType(contractFields)

      val contractdf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(contractSchema).load(contractHistoryPath)

      val contractdfFilterd = contractdf.filter(col("EVENT") === "Status Update" || col("EVENT") === "Quote-Association")
      logger.info("filteredCount:" + contractdfFilterd.count())

      val convertStrToDateUDF = udf(convertStrToDate _)
      val convertSFDCDTStrToDateUDF = udf(convertSFDCDTStrToDate _)
      var convertModificaitonToFinalStatusUDF = udf(convertModificaitonToFinalStatus _)
      val contractdfWithDT = contractdfFilterd.withColumn("MODIFICATION_DATE_DT", convertStrToDateUDF(col("MODIFICATION_DATE"))).
        withColumn("Status", convertModificaitonToFinalStatusUDF(col("MODIFICATION"))).
        drop("MODIFICATION_DATE").
        drop("MODIFICATION")

      val contractdfWithDTWithStatusAgggregated = contractdfWithDT.groupBy("Status", "QUOTE_ID", "EVENT").agg(min("MODIFICATION_DATE_DT") as "firstStatusDate", max("MODIFICATION_DATE_DT") as "lastStatusDate").orderBy("Status", "QUOTE_ID", "EVENT")
      val contractdfCDA = contractdfWithDTWithStatusAgggregated.filter(col("Status") === "CD-Approved")
      val contractdfCDW = contractdfWithDTWithStatusAgggregated.filter(col("Status") === "Downloaded")
      val contractdfCREATE = contractdfWithDTWithStatusAgggregated.filter(col("Status") === "Creation")
      //println(contractdfCREATE.rdd.partitions.size)
      val contractDataWOCreation = contractdfCDW.join(contractdfCDW, contractdfCDW("QUOTE_ID") === contractdfCDA("QUOTE_ID")).
        select(
          contractdfCDA("QUOTE_ID") as "QUOTE_ID",
          contractdfCDA("firstStatusDate") as "CDAFirstStatusDate",
          contractdfCDA("lastStatusDate") as "CDAlastStatusDate",
          contractdfCDW("firstStatusDate") as "CDWFirstStatusDate",
          contractdfCDW("lastStatusDate") as "CDWlastStatusDate")
      val contractDataMS = contractDataWOCreation.join(contractdfCREATE, contractDataWOCreation("QUOTE_ID") === contractdfCREATE("QUOTE_ID"), "rightouter").
        select(
          contractdfCREATE("QUOTE_ID"),
          contractDataWOCreation("CDAFirstStatusDate"),
          contractDataWOCreation("CDAlastStatusDate"),
          contractDataWOCreation("CDWFirstStatusDate"),
          contractDataWOCreation("CDWlastStatusDate"),
          contractdfCREATE("firstStatusDate") as "CreationDate")
      
      logger.info("After Joins and Filters")  
      contractDataMS.show()
      def timeToStr(milliSec: Long): String = DateTimeFormat.forPattern("MM/dd/YYYY").print(milliSec)
      val eventstartDateDate = udf(timeToStr _)

      val contractDataFinal = contractDataMS.withColumn("CreationDateStr", eventstartDateDate(contractDataMS("CreationDate")))

      val contractDataFinalOne = contractDataFinal.rdd
      contractDataFinalOne.saveAsTextFile(outputfileLoc)
      //println(contractDataFinal.count())
      //optyDFRaw.cache()
      //Setting up Opty Data
      //val optyDFRaw = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(optySchema).load(optyHistoryPath)
      //val optyDFWithDT = optyDFRaw.withColumn("CreatedDate_DT", convertSFDCDTStrToDateUDF(col("CreatedDate"))).drop("CreatedDate")
      //val optyDFWithDTGrouped = optyDFWithDT.groupBy("OpportunityId").agg(min("CreatedDate_DT") as "optycreateddate")

    } finally {
      sc.stop()
    }
  }
}