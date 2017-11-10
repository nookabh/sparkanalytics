package com.workflow.cx

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import com.workflow.rt.UtilityClass
import org.apache.spark.sql.functions._

class VPApprovalVol(base_df: DataFrame) extends CXTransformationBase {
  /* @author Bharath
   	Metric : VP Approval Volume  Metric
 	*/
  //  val base_df = this.getBaseDF()
  try {
    val head = "\"SFDCOppId\",\"EventName\",\"EventType\",\"EventStartDate\",\"EventCompletionDate\",\"Count\"" + System.lineSeparator
    this.writeFile(calculateVPApprovalVolume, "VPAPPROVAL_VOL", head)
    def calculateVPApprovalVolume(): DataFrame = {
      val substring = udf(UtilityClass.substringFn _)
      val findDiffUDF = udf(UtilityClass.findDiff _)
      val parse_df = base_df.withColumn("parsedDate", findDiffUDF(base_df("milestone_date")))
      val draft_df = parse_df.filter(base_df("MILESTONE_ID") === "10251070" //get the latest
        || base_df("MILESTONE_ID") === "10301070").select("Quote_id", "short_oppty_id", "milestone_date", "parsedDate").withColumn("EventCompletionDate", substring(col("milestone_date")))
        .drop(col("milestone_date")).drop(col("parsedDate"))
      val out_df = draft_df
        .where(col("short_oppty_id") !== "")
        .groupBy("short_oppty_id", "EventCompletionDate")
        .agg(count("Quote_id") as "Count")
        .withColumn("EventType", lit("ROLLING"))
        .withColumn("EventName", lit("VPAPPROVAL_VOL"))
        .withColumn("EventStartDate", lit(""))
        val finaldf = out_df.select("short_oppty_id", "EventName", "EventType", "EventStartDate", "EventCompletionDate", "Count")
      out_df
    }
  } catch {
    case e: Exception          => { println("Common Exception Occured:" + e); null }
    case _: ClassCastException => { println("some other exception occured"); null }
  }
}
