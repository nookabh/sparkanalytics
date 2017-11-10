package com.workflow.cx

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.joda.time.format.DateTimeFormat
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql._
import scala.collection.mutable
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashSet
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.storage.StorageLevel
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.SQLContext
import com.workflow.rt.UtilityClass
import java.sql.Date
import scala.collection.mutable.ListBuffer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column

class PRCount(base_df: DataFrame, sqlContext: SQLContext) extends CXTransformationBase {
  try {
    val head = "\"SFDCOppId\"|\"PR1\"|\"PR2\"|\"PR3\"|\"PR4\"" + System.lineSeparator
    //this.writeFile(calculatePRCount, "OPP_PRODUCTS", head)
    writePRCpunts(calculatePRCount, "OPP_PRODUCTS")
    def writePRCpunts(outDF: DataFrame, metricName: String) {
      val outputfileLoc = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/out/metrics/"
      //val outputfileLoc = "C:/ac/TestData/scala/out/metrics/"
      val outFileName = outputfileLoc + metricName
      println(outFileName)
      outDF//.coalesce(2)
        .write.format("com.databricks.spark.csv")
        .mode("overwrite") //.option("header", "true")
        .option("treatEmptyValuesAsNulls", "true")
        .option("delimiter", "|")
        .save(outFileName)
        this.merge(metricName, head)
    }

    def createGroupedDF(df: DataFrame, sqlContextHiv: SQLContext, columnName: String): DataFrame = {
      val rdd: RDD[(String, String)] = df.map(row => (row.getAs[String](0), row.getAs[String](1)))

      val initialSet = mutable.HashSet.empty[String]
      val addToSet = (s: mutable.HashSet[String], v: String) => s += v
      val mergePartitionSets = (p1: mutable.HashSet[String], p2: mutable.HashSet[String]) => p1 ++= p2

      val uniqueByKey = rdd.aggregateByKey(initialSet)(addToSet, mergePartitionSets).mapValues(_.toList)
      import sqlContext.implicits._
      val newNames = Seq("id", "x1")

      val finalDF = uniqueByKey.toDF()
      val renameAndSemiComunUDF = udf(UtilityClass.renameAndSemiComun _)
      val finalDF2 = finalDF.withColumn("OppId", col("_1")).drop("_1").withColumn(columnName, renameAndSemiComunUDF(col("_2"))).drop("_2")
      finalDF2
    }

    def calculatePRCount(): DataFrame = {
      //val sqlContext: SQLContext = new HiveContext(sc)
      val parseToDate = udf(UtilityClass.dateStrToDateTime _)
      val comcatStringUDF = udf((a: String, b: String) => a + "-" + b)
      val dateStrToWeekendDate = udf(UtilityClass.dateStrToWeekendDate _)

      val base_df_formatted_quote = base_df //.where("SFDC_OPPORTUNITY_ID__C" == " ")
      base_df_formatted_quote.registerTempTable("base_df_formatted_quote")
      val base_df_grouped_quote = base_df_formatted_quote.groupBy("SFDC_OPPORTUNITY_ID__C").count()
      val base_pricing_df = base_df.groupBy(base_df("SFDC_OPPORTUNITY_ID__C")).agg(collect_set("PRODUCT_FAMILY_PR4__C") as "PR4", collect_set("PRODUCT_GROUP_PR3__C") as "PR3", collect_set("PR2__C") as "PR2", collect_set("PRM_PRODUCT_ROLLUP_1_PR1__C") as "PR1")
      val pr4DF = createGroupedDF(base_df.select(col("SFDC_OPPORTUNITY_ID__C"), col("PRODUCT_FAMILY_PR4__C")), sqlContext, "PR4")
      val pr3DF = createGroupedDF(base_df.select(col("SFDC_OPPORTUNITY_ID__C"), col("PRODUCT_GROUP_PR3__C")), sqlContext, "PR3")
      val pr2DF = createGroupedDF(base_df.select(col("SFDC_OPPORTUNITY_ID__C"), col("PR2__C")), sqlContext, "PR2")
      val pr1DF = createGroupedDF(base_df.select(col("SFDC_OPPORTUNITY_ID__C"), col("PRM_PRODUCT_ROLLUP_1_PR1__C")), sqlContext, "PR1")
      val j1 = pr1DF.join(pr2DF, pr2DF("OppId") === pr1DF("OppId")).select(pr1DF("OppId"), col("PR1"), col("PR2"))
      val j2 = pr3DF.join(pr4DF, pr3DF("OppId") === pr4DF("OppId")).select(pr3DF("OppId"), col("PR3"), col("PR4"))
      val finalDF_before_quoteCounts = j1.join(j2, j1("OppId") === j2("OppId")).select(j1("OppId"), col("PR1"), col("PR2"), col("PR3"), col("PR4"))
      finalDF_before_quoteCounts
    }
  } catch {
    case e: Exception          => { println("Common Exception Occured:" + e); null }
    case _: ClassCastException => { println("some other exception occured"); null }
  }
}