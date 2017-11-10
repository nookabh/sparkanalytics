package com.workflow.cx

/* @author cholaan
   	Metric : Contract Initiated To Signed
   	Max of Contract Signed Event Received - Min of one among (10601013/10601015)
   		10601013 - Draft Contracting with Pricing Initiation Success from GCT (For Draft)
      10601015 - Contracting Initiation Success from GCT (For Final)
      10601020 - Contract Signed Event Received
   	
 */

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import com.workflow.rt.UtilityClass
import org.apache.spark.sql.functions._

class ContractingTime(base_df: DataFrame) extends CXTransformationBase {
  try {
    val head = "\"SFDCOppId\",\"EventName\",\"EventType\",\"EventStartDate\",\"EventCompletionDate\",\"Count\"" + System.lineSeparator
    this.writeFile(claculateContractingTime, "CONTRACTING_TIME",head)
    def claculateContractingTime() = {
      val timeToStrUDF = udf(UtilityClass.timeToStr _)
      val diffUDF = udf(UtilityClass.minus _)
      val dateToMillSecondUDF = udf(UtilityClass.dateToMillSecond _)
      val changeSecToDay = udf(UtilityClass.millisecondToDay _)
      val removeComma = udf(UtilityClass.removeCommaFormatter _)
      val parsed_df = base_df.withColumn("parsedDate", dateToMillSecondUDF(base_df("milestone_date"))).withColumn("milestoneId", removeComma(base_df("MILESTONE_ID")))
      val contractInitated_df = parsed_df.filter((parsed_df("milestoneId") === "10601013" || parsed_df("milestoneId") === "10601015"))
      val signed_df = parsed_df.filter(parsed_df("milestoneId") === "10601020")
      val contractMin_df = contractInitated_df
        .groupBy("short_oppty_id").agg(min("parsedDate") as "EventStartDate")
      val signedMax_df = signed_df
        .groupBy("short_oppty_id").agg(max("parsedDate") as "EventEndDate")
      val join_df = contractMin_df.join(signedMax_df, "short_oppty_id")
      val result_df = join_df.withColumn("Day Spent MilliSeconds", diffUDF(join_df("EventEndDate"), join_df("EventStartDate")))
      val final_df = result_df.withColumn("Count", changeSecToDay(result_df("Day Spent MilliSeconds")))
        .withColumn("SFDCOppId", result_df("short_oppty_id"))
        .withColumn("EventStartDate", timeToStrUDF(result_df("EventStartDate")))
        .withColumn("EventCompletionDate", timeToStrUDF(result_df("EventEndDate")))
        .withColumn("EventName", lit("CONTRACTING_TIME"))
        .withColumn("EventType", lit("ROLLING"))
      val contractTime_df = final_df.select("SFDCOppId", "EventName", "EventType", "EventStartDate", "EventCompletionDate", "Count")
      contractTime_df
    }
  } catch {
    case e: Exception          => { println("Common Exception Occured:" + e); null }
    case _: ClassCastException => { println("some other exception occured"); null }
  }
}
