package com.workflow.cx

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import com.workflow.rt.UtilityClass
import org.apache.spark.sql.functions._
import scala.collection.mutable.ListBuffer
import java.sql.Date

class MultiQuoteSets(base_df: DataFrame, history_df: DataFrame) extends CXTransformationBase {
  /* @author cholaan
   	Metric :MultiQuoteSets
 */
  try {
    val head = "\"SFDCOppId\",\"EventName\",\"EventType\",\"EventStartDate\",\"EventCompletionDate\",\"Count\"" + System.lineSeparator
    this.writeFile(getMultiQuoteSets, "MULTI_QUOTE_SET", head)
    def getMultiQuoteSets(): DataFrame = {
      val findDiffUDF = udf(UtilityClass.minus _)
      val timeToStrUDF = udf(UtilityClass.timeToStr _)
      val dateToMillSecondUDF = udf(UtilityClass.dateToMillSecond _)
      val dateTimeToMillSecondUDF = udf(UtilityClass.dateTimeToMillSecond _)
      val changeSecToDay = udf(UtilityClass.millisecondToDay _)
      val getEndDayOfWeekUDF = udf(UtilityClass.getEndDayOfWeek _)
      val divisionUDF = udf(UtilityClass.division _)
      val convertDoubleToLongUDF = udf(UtilityClass.convertDoubleToLong _)
      

      val historyFilte_df = history_df
        .filter(history_df("STAGENAME") === "2 Solve" || history_df("STAGENAME") === "3 Propose" || history_df("STAGENAME") === "4 Negotiate")
        .where(history_df("SFDC_OPPORTUNITY_ID__C") !== "")
        .sort("SFDC_OPPORTUNITY_ID__C")
        .groupBy("SFDC_OPPORTUNITY_ID__C").count().distinct
      val multiQuore_df = base_df.filter(base_df("PP_Quote_Type") === "Linked Quote")
      val baseQuoteCount_df = multiQuore_df
        .where(col("short_oppty_id") !== "")
        .sort("short_oppty_id")
      val baseCountWithWeek_df = baseQuoteCount_df
        .groupBy("short_oppty_id", "base_quote_id").agg(count("base_quote_id") as "BaseQuoteCount").distinct
      val PCMFilter_df = multiQuore_df
        .filter(multiQuore_df("MILESTONE_ID") === "10251109" || multiQuore_df("MILESTONE_ID") === "10251110" || multiQuore_df("MILESTONE_ID") === "10301109" || multiQuore_df("MILESTONE_ID") === "10301110")
        .sort("short_oppty_id")
      val pcmCountWithWeek_df = PCMFilter_df
        .groupBy("short_oppty_id", "base_quote_id").agg(count("base_quote_id") as "PCMBaseQuoteCount").distinct
      val count_join = baseCountWithWeek_df.join(pcmCountWithWeek_df,
        baseCountWithWeek_df("short_oppty_id") === pcmCountWithWeek_df("short_oppty_id") &&
          baseCountWithWeek_df("base_quote_id") === pcmCountWithWeek_df("base_quote_id"))
      val pcmQuote_df = count_join.drop(pcmCountWithWeek_df("short_oppty_id")).drop(pcmCountWithWeek_df("base_quote_id"))
      val join_df = pcmQuote_df.join(historyFilte_df, historyFilte_df("SFDC_OPPORTUNITY_ID__C") === pcmQuote_df("short_oppty_id"))
      val finalPCM_df = join_df.drop("SFDC_OPPORTUNITY_ID__C").drop("count").orderBy("short_oppty_id")
      val baseQuote_df = finalPCM_df.groupBy("short_oppty_id").agg(sum("BaseQuoteCount"), sum("PCMBaseQuoteCount"))
        .withColumn("Count", divisionUDF(col("sum(PCMBaseQuoteCount)"), col("sum(BaseQuoteCount)")))
        .withColumn("EventStartDate", lit(""))
        .withColumn("EventCompletionDate", lit(""))
        .withColumn("EventName", lit("MULTI_QUOTE_SET"))
        .withColumn("EventType", lit("ROLLING"))
      val out_df = baseQuote_df.select("short_oppty_id", "EventName", "EventType", "EventStartDate", "EventCompletionDate", "Count")
      out_df
    }
  } catch {
    case e: Exception          => { println("Common Exception Occured:" + e); null }
    case _: ClassCastException => { println("some other exception occured"); null }
  }
}
