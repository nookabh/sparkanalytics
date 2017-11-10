package com.workflow.cx

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import com.workflow.rt.UtilityClass
import org.apache.spark.sql.functions._

class QuotesPerMultiQuoteSet(base_df: DataFrame) extends CXTransformationBase {
  /* @author cholaan
   	Metric : Quotes per Multi Quote Set
 	*/
  //  val base_df = this.getBaseDF()
  try {
    val head = "\"SFDCOppId\",\"EventName\",\"EventType\",\"EventStartDate\",\"EventCompletionDate\",\"Count\"" + System.lineSeparator
    this.writeFile(calculateQuotesPerMultiQuoteSet, "QUOTES_PER_MULTI_QUOTE", head)
    def calculateQuotesPerMultiQuoteSet(): DataFrame = {
      //parsed_df.filter(base_df("short_oppty_id"))
      val getWeekFromDateUDF = udf(UtilityClass.getWeekFromDate _)
      val divisionUDF = udf(UtilityClass.division _)
      val getStartDayOfWeekUDF = udf(UtilityClass.getStartDayOfWeek _)
      val getEndDayOfWeekUDF = udf(UtilityClass.getEndDayOfWeek _)
      val dateToMillSecondUDF = udf(UtilityClass.dateToMillSecond _)
      val timeToStrUDF = udf(UtilityClass.timeToStr _)
      val convertDoubleToLongUDF = udf(UtilityClass.convertDoubleToLong _)
      val timeStringToStrUDF = udf(UtilityClass.timeStringToStr _)
      val parsed_df = base_df.filter(base_df("PP_Quote_Type") === "Linked Quote")
        .where(col("short_oppty_id") !== "")
        .drop(base_df("Quote_version"))
        .drop(base_df("Long_oppty_id"))
        .drop(base_df("milestone_date"))
        .drop(base_df("MILESTONE_ID"))
      val optGropu_df = parsed_df.groupBy("short_oppty_id", "base_quote_id").count()

      val baseQuote_df = optGropu_df.groupBy("short_oppty_id").agg(sum("Count") as "TotalQuoteCount", count("short_oppty_id") as "BaseQuoteCount")
        .withColumn("Count", divisionUDF(col("TotalQuoteCount"), col("BaseQuoteCount")))
        .withColumn("EventStartDate", lit(""))
        .withColumn("EventCompletionDate", lit(""))
        .withColumn("EventName", lit("QUOTES_PER_MULTI_QUOTE"))
        .withColumn("EventType", lit("STATIC"))

      val out_df = baseQuote_df.select("short_oppty_id", "EventName", "EventType", "EventStartDate", "EventCompletionDate", "Count")
      out_df
    }
  } catch {
    case e: Exception          => { println("Common Exception Occured:" + e); null }
    case _: ClassCastException => { println("some other exception occured"); null }
  }
}
