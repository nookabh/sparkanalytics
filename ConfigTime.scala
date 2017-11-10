package com.workflow.cx

/* @author cholaan
   	Metric : Quote Created To Last PriceRequest
   	Max of last Price Request - Min of one among (10001010/10001088/10001410)
   		10001010 - Quote Creation in Progress (All v0 Quotes)
			10001088 - Create Revision - Being Processed (Non-Smart Quote Revision)
			10001410 - Smart Quote Process Started (Smart Quote Revision)
			10301010 - Price Quote Requested
   	
 */
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import com.workflow.rt.UtilityClass
import org.apache.spark.sql.functions._

class ConfigTime(base_df: DataFrame) extends CXTransformationBase {
  try {
    val head = "\"SFDCOppId\",\"EventName\",\"EventType\",\"EventStartDate\",\"EventCompletionDate\",\"Count\"" + System.lineSeparator
    this.writeFile(calculeteConfigTime, "CONFIG_TIME",head)
    def calculeteConfigTime(): DataFrame = {
      val timeToStrUDF = udf(UtilityClass.timeToStr _)
      val diffUDF = udf(UtilityClass.minus _)
      val dateToMillSecondUDF = udf(UtilityClass.dateToMillSecond _)
      val changeSecToDay = udf(UtilityClass.millisecondToDay _)
      val removeComma = udf(UtilityClass.removeCommaFormatter _)
      val parsed_df = base_df.withColumn("parsedDate", dateToMillSecondUDF(base_df("milestone_date"))).withColumn("milestoneId", removeComma(base_df("MILESTONE_ID")))
      val quoteCreated_df = parsed_df.filter((parsed_df("milestoneId") === "10001010" || parsed_df("milestoneId") === "10001088" || parsed_df("milestoneId") === "10001410"))
      val lastPriced_df = parsed_df.filter((parsed_df("milestoneId") === "10301010"))
      val contractMin_df = quoteCreated_df.groupBy("short_oppty_id").agg(min("parsedDate") as "EventStartDate")
      val signedMax_df = lastPriced_df.groupBy("short_oppty_id").agg(max("parsedDate") as "EventEndDate")
      val join_df = contractMin_df.join(signedMax_df, "short_oppty_id")
      val result_df = join_df.withColumn("Day Spent MilliSeconds", diffUDF(join_df("EventEndDate"), join_df("EventStartDate")))
      val final_df = result_df.withColumn("Count", changeSecToDay(result_df("Day Spent MilliSeconds")))
        .withColumn("SFDCOppId", result_df("short_oppty_id"))
        .withColumn("EventStartDate", timeToStrUDF(result_df("EventStartDate")))
        .withColumn("EventCompletionDate", timeToStrUDF(result_df("EventEndDate")))
        .withColumn("EventName", lit("CONFIG_TIME"))
        .withColumn("EventType", lit("ROLLING"))
      val return_df = final_df.select("SFDCOppId", "EventName", "EventType", "EventStartDate", "EventCompletionDate", "Count")
      return_df
    }
  } catch {
    case e: Exception          => { println("Common Exception Occured:" + e); null }
    case _: ClassCastException => { println("some other exception occured"); null }
  }
}
