package com.workflow.cx

/* @author cholaan
   	Metric : Contract Initiated To Signed
   		10001010 - Quote Creation in Progress (All v0 Quotes)
			10001088 - Create Revision - Being Processed (Non-Smart Quote Revision)
			10001410 - Smart Quote Process Started (Smart Quote Revision)
			10601012 - Draft Contracting without Pricing Initiation Success from GCT
		Max of Draft Contracting without Pricing Initiation - Min of one among first three (10001010/10001088/10001410)
 	*/

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import com.workflow.rt.UtilityClass
import org.apache.spark.sql.functions._

class InitialDraftContract(base_df: DataFrame) extends CXTransformationBase {
  try {
 val head = "\"SFDCOppId\",\"EventName\",\"EventType\",\"EventStartDate\",\"EventCompletionDate\",\"Count\"" + System.lineSeparator
  this.writeFile(calculateInitialDraftContract, "INITIAL_DRAFT_CONTRACT",head)
    /*writeFileForIntDraft(calculateInitialDraftContract, "INITIAL_DFT_CONTRACT", outputfileLoc)
    def writeFileForIntDraft(outDF: DataFrame, metricName: String, outputfileLoc: String) {
      val outFileName = outputfileLoc + metricName
      outDF.coalesce(2)
        .write.format("com.databricks.spark.csv")
        .mode("overwrite") //.option("header", "true")
        .option("treatEmptyValuesAsNulls", "true")
        .save(outFileName)
        this.merge(metricName, head)
    }*/
    def calculateInitialDraftContract(): DataFrame = {
      val findDiffUDF = udf(UtilityClass.minus _)
      val timeToStrUDF = udf(UtilityClass.timeToStr _)
      val fullDateToMillSecondUDF = udf(UtilityClass.fullDateToMillSecond _)
      val changeSecToDay = udf(UtilityClass.millisecondToDay _)
      val removeComma = udf(UtilityClass.removeCommaFormatter _)
      val parsed_df = base_df.withColumn("parsedDate", fullDateToMillSecondUDF(base_df("milestone_date"))) //.withColumn("milestoneId", removeComma(base_df("MILESTONE_ID")))
      val start_df = parsed_df.filter((parsed_df("MILESTONE_ID") === "10601012"))
      val startMax_df = start_df.select("short_oppty_id", "parsedDate").groupBy("short_oppty_id").agg(max("parsedDate") as "EventEndDate")
      val approved_df = parsed_df.filter(parsed_df("MILESTONE_ID").equalTo("10001010") || parsed_df("MILESTONE_ID").equalTo("10001088") || parsed_df("MILESTONE_ID").equalTo("10001410"))
      val approvedMin_df = approved_df
        .groupBy("short_oppty_id").agg(min("parsedDate") as "EventStartDate")
      val join_df = approvedMin_df.join(startMax_df, "short_oppty_id")
      val result_df = join_df.withColumn("Day Spent MilliSeconds", join_df("EventEndDate") - join_df("EventStartDate"))
      val final_df = result_df.withColumn("Count", changeSecToDay(result_df("Day Spent MilliSeconds")))
        .withColumn("SFDCOppId", result_df("short_oppty_id"))
        .withColumn("EventStartDate", timeToStrUDF(result_df("EventStartDate")))
        .withColumn("EventCompletionDate", timeToStrUDF(result_df("EventEndDate")))
        .withColumn("EventName", lit("INITIAL_DRAFT_CONTRACT"))
        .withColumn("EventType", lit("ROLLING"))
      val return_df = final_df.select("SFDCOppId", "EventName", "EventType", "EventStartDate", "EventCompletionDate", "Count")
      //println("Out put Row count==>" + return_df.count())
      //return_df.show()
      return_df
    }
  } catch {
    case e: Exception          => { println("Common Exception Occured:" + e); null }
    case _: ClassCastException => { println("some other exception occured"); null }
  }
}
