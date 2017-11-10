package com.workflow.cx

/* @author bnookala
   * NVDW-491
    "10251060 - Rate and Term Approval Requested (RTB)
    10301060 - Price Quote Approval Requested (TX)
    
    10251070 - Rate and Term Entitlement Discounts Approved (RTB)
    10301070 - Price Quote Entitlement Discounts Approved (TX)"
 */

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import com.workflow.rt.UtilityClass
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.min
import org.apache.spark.sql.functions.udf
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.functions._
import java.util.Calendar

class VPApprovalMetric(base_df: DataFrame) extends CXTransformationBase {
  try {
    val head = "\"SFDCOppId\",\"EventName\",\"EventType\",\"EventStartDate\",\"EventCompletionDate\",\"Count\"" + System.lineSeparator
    this.writeFile(calculateVPApprovalMetric, "VP_APPROVAL",head)
    def calculateVPApprovalMetric(): DataFrame = {
      val findDiffUDF = udf(UtilityClass.findDiff _)
      val parsed_df = base_df.withColumn("parsedDate", findDiffUDF(base_df("milestone_date")))
      val created_df = parsed_df.filter(base_df("MILESTONE_ID") === "10251060"
        || base_df("MILESTONE_ID") === "10301060") //get the min
      val draft_df = parsed_df.filter(base_df("MILESTONE_ID") === "10251070"
        || base_df("MILESTONE_ID") === "10301070") //get the latest
      val createdMin_df = created_df.groupBy("short_oppty_id").agg(min("parsedDate") as "StartDate")
      val draftMax_df = draft_df.groupBy("short_oppty_id").agg(max("parsedDate") as "EndDate")
      val final_df = createdMin_df.join(draftMax_df, draftMax_df("short_oppty_id") === createdMin_df("short_oppty_id"))
      val finaldataframe = final_df.withColumn("SFDCOppId", createdMin_df("short_oppty_id"))
      val result_df = finaldataframe.withColumn("TimeDifference", finaldataframe("EndDate") - finaldataframe("StartDate"))
      val timedifference = udf(UtilityClass.millisecondToDay _)
      val parsetimediff = result_df.withColumn("Count", timedifference(result_df("TimeDifference")))
      val parser = udf(UtilityClass.timeToStr _)
      val parsedeventstartdate = parsetimediff.withColumn("EventStartDate", parser(result_df("StartDate")))
      val parsedEventCompletionDate = parsedeventstartdate.withColumn("EventCompletionDate", parser(result_df("EndDate")))
        .withColumn("EventType", lit("ROLLING"))
        .withColumn("EventName", lit("VP_APPROVAL"))
      val result = parsedEventCompletionDate.select("SFDCOppId", "EventName", "EventType", "EventStartDate", "EventCompletionDate", "Count")
      result
    }
  } catch {
    case e: Exception          => { println("Common Exception Occured:" + e); null }
    case _: ClassCastException => { println("some other exception occured"); null }
  }
}
