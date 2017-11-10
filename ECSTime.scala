package com.workflow.cx

/* @author cholaan
    * Metric : The time between ECS start and ECS Approval Received
    * Max of 
    		10401030 - Pending Review with IEC
        10401035 - Export Check Completed
        10401040 - Export Check Completed
        10401050 - Export Check Completed (Most Common)
        10401060 - Export Check Completed
        10401070 - Export Check Completed
     Min of 10401005 - ECS request initiated
   
 		*/

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import com.workflow.rt.UtilityClass
import org.apache.spark.sql.functions._

class ECSTime(base_df: DataFrame) extends CXTransformationBase {
  try {
    val head = "\"SFDCOppId\",\"EventName\",\"EventType\",\"EventStartDate\",\"EventCompletionDate\",\"Count\"" + System.lineSeparator
    this.writeFile(calculateECSTime, "ECS_TIME",head)
    def calculateECSTime(): DataFrame = {
      val findDiffUDF = udf(UtilityClass.minus _)
      val timeToStrUDF = udf(UtilityClass.timeToStr _)
      val dateToMillSecondUDF = udf(UtilityClass.dateToMillSecond _)
      val changeSecToDay = udf(UtilityClass.millisecondToDay _)
      val removeComma = udf(UtilityClass.removeCommaFormatter _)
      val parsed_df = base_df.withColumn("parsedDate", dateToMillSecondUDF(base_df("milestone_date"))).withColumn("milestoneId", removeComma(base_df("MILESTONE_ID")))
      val start_df = parsed_df.filter((parsed_df("milestoneId") === "10401005"))
      val approved_df = parsed_df.filter((parsed_df("milestoneId") === "10401030"
        || parsed_df("milestoneId") === "10401035"
        || parsed_df("milestoneId") === "10401040"
        || parsed_df("milestoneId") === "10401050"
        || parsed_df("milestoneId") === "10401060"
        || parsed_df("milestoneId") === "10401070"))
      val startMin_df = start_df.select("short_oppty_id", "parsedDate").groupBy("short_oppty_id").agg(min("parsedDate") as "EventStartDate")
      val approvedMax_df = approved_df
        .select(approved_df("short_oppty_id"), approved_df("parsedDate"))
        .groupBy("short_oppty_id").agg(max("parsedDate") as "EventEndDate")
      val join_df = startMin_df.join(approvedMax_df, "short_oppty_id")
      val result_df = join_df.withColumn("Day Spent MilliSeconds", findDiffUDF(join_df("EventEndDate"), join_df("EventStartDate")))
      val final_df = result_df.withColumn("Count", changeSecToDay(result_df("Day Spent MilliSeconds")))
        .withColumn("SFDCOppId", result_df("short_oppty_id"))
        .withColumn("EventStartDate", timeToStrUDF(result_df("EventStartDate")))
        .withColumn("EventCompletionDate", timeToStrUDF(result_df("EventEndDate")))
        .withColumn("EventName", lit("ECS_TIME"))
        .withColumn("EventType", lit("ROLLING"))
      val return_df = final_df.select("SFDCOppId", "EventName", "EventType", "EventStartDate", "EventCompletionDate", "Count")
      return_df
    }
  } catch {
    case e: Exception          => { println("Common Exception Occured:" + e); null }
    case _: ClassCastException => { println("some other exception occured"); null }
  }
}
