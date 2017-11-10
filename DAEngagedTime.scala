package com.workflow.cx

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import com.workflow.rt.UtilityClass
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row

class DAEngagedTime(base_df: DataFrame) extends CXTransformationBase {
  try {
    val head = "\"SFDCOppId\",\"EventName\",\"EventType\",\"EventStartDate\",\"EventCompletionDate\",\"Count\"" + System.lineSeparator
    this.writeFile(calculateDAEngagedTime, "DA_ENGAGED_TIME", head)
    def calculateDAEngagedTime(): DataFrame = {
      val findDiffUDF = udf(UtilityClass.minus _)
      val timeToStrUDF = udf(UtilityClass.timeToStr _)
      val dateToMillSecondUDF = udf(UtilityClass.dateToMillSecond _)
      val changeSecToDay = udf(UtilityClass.millisecondToDay _)
      val filter_df = base_df
        .filter(base_df("TEAM_INITIALS__C") === "DA" && base_df("NEWVALUE") === "Release to Sales/Pricing")
        .filter(base_df("STAGENAME") === "2 Solve" || base_df("STAGENAME") === "3 Propose" || base_df("STAGENAME") === "4 Negotiate")
      val temp_df = filter_df
        .withColumn("EventStartDateMS", dateToMillSecondUDF(base_df("Team_Form_date")))
        .withColumn("Team_Form_Hist_date_ms", dateToMillSecondUDF(base_df("Team_Form_Hist_date")))
      val maxTime_fd = temp_df.groupBy("SFDC_OPPORTUNITY_ID__C").agg(max("Team_Form_Hist_date_ms") as "maxDate") //.show()
      val join_df = temp_df.join(maxTime_fd, maxTime_fd("maxDate") === temp_df("Team_Form_Hist_date_ms"), "right").sort(maxTime_fd("SFDC_OPPORTUNITY_ID__C"))
      val final_df = join_df.withColumn("SFDCOppId", temp_df("SFDC_OPPORTUNITY_ID__C"))
        .withColumn("Count", changeSecToDay(findDiffUDF(join_df("maxDate"), join_df("EventStartDateMS"))))
        .withColumn("EventCompletionDate", timeToStrUDF(join_df("maxDate")))
        .withColumn("EventStartDate", timeToStrUDF(join_df("EventStartDateMS")))
        .withColumn("EventName", lit("DA_ENGAGED_TIME"))
        .withColumn("EventType", lit("ROLLING"))
        .dropDuplicates(Array("SFDCOppId", "EventCompletionDate"))
      val return_df = final_df.select("SFDCOppId", "EventName", "EventType", "EventStartDate", "EventCompletionDate", "Count")
      return_df
    }
  } catch {
    case e: Exception          => { println("Common Exception Occured:" + e); null }
    case _: ClassCastException => { println("some other exception occured"); null }
  }
}
