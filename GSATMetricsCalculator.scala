package com.workflow.cx

/* @author girish - GSATMetric
   * 
   */

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import com.workflow.rt.UtilityClass
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.min
import org.apache.spark.sql.functions.udf
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.functions._
import java.util.Calendar

class GSATMetricsCalculator(base_df: DataFrame) extends CXTransformationBase {
  try {

    val head = "\"SFDCOppId\",\"EventName\",\"EventType\",\"EventStartDate\",\"EventCompletionDate\",\"Count\"" + System.lineSeparator
    this.writeFile(calculateGSATsMetrics, "GSAT_METRICS", head)
    def calculateGSATsMetrics(): DataFrame = {
     val calculateGSATMetricName = udf(UtilityClass.calculateGSATMetrics _)
      var completedGSATs = base_df
        .filter(base_df("COMPLETED_DATE__C") !== "")
        .filter(base_df("COMPLETED_DATE__C").isNotNull)
        .filter(base_df("SLO_COMMITMENT_DATE__C") !== "")
        .filter(base_df("SLO_COMMITMENT_DATE__C").isNotNull)
        .withColumn("EventStartDate", lit(""))
        .withColumn("Count", lit("1"))
        .filter(base_df("ID") !== "ID")
      completedGSATs = completedGSATs.withColumn("EventType", lit("STATIC"))
        .withColumn("EventName", calculateGSATMetricName(base_df("CREATEDDATE"), base_df("SLO_COMMITMENT_DATE__C"), base_df("COMPLETED_DATE__C")))
        .withColumn("SFDCOppId", base_df("OPPORTUNITY_ID_SHORT__C"))
        .withColumn("EventComplDate", base_df("COMPLETED_DATE__C")).orderBy("COMPLETED_DATE__C")
      val finalDF = completedGSATs.select("SFDCOppId", "EventName", "EventType", "EventStartDate", "EventComplDate", "Count").distinct()
      finalDF
    }
  } catch {
    case e: Exception          => { println("Common Exception Occured:" + e); null }
    case _: ClassCastException => { println("some other exception occured"); null }
  }
}
