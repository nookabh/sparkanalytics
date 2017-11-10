package com.workflow.cx
/* @author cholaan
   	Metric : SitesPerMultiQuoteSet
 	*/
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import com.workflow.rt.UtilityClass
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext

class SitesPerMultiQuoteSet(pqDF: DataFrame, siteDF: DataFrame) extends CXTransformationBase {
  try {

    val head = "\"SFDCOppId\",\"EventName\",\"EventType\",\"EventStartDate\",\"EventCompletionDate\",\"Count\"" + System.lineSeparator
    this.writeFile(calculateSitePerMultiQuoteSet, "SITE_PER_MULTI_QUOTE", head)
    def calculateSitePerMultiQuoteSet(): DataFrame = {
      val getEndDayOfWeekUDF = udf(UtilityClass.getEndDayOfWeek _)
      val dateToMillSecondUDF = udf(UtilityClass.dateToMillSecond _)
      val multiQuoteDF = pqDF.filter(pqDF("PP_Quote_Type") === "Linked Quote")
        .groupBy(pqDF("short_oppty_id"), pqDF("Quote_id")).agg(count("Quote_id") as "QuoteCount", first("linked_quote_create_date") as "QuoteCreatedDate")
      val siteQuoteDF = siteDF.groupBy(siteDF("OPTY_ID"), siteDF("QTE_ID")).agg(count("QTE_ID") as "Site_Count").sort("QTE_ID")
      val joinDF = siteQuoteDF.join(multiQuoteDF, siteQuoteDF("QTE_ID") === multiQuoteDF("Quote_id"), "inner")
        .withColumn("EventStartDate", multiQuoteDF("QuoteCreatedDate").substr(0, 10))
        .withColumn("EventCompletionDate", lit(""))
        .withColumn("EventName", lit("SITE_PER_MULTI_QUOTE"))
        .withColumn("EventType", lit("ROLLING"))

      val finalDF = joinDF.select("short_oppty_id", "EventName", "EventType", "EventStartDate", "EventCompletionDate", "Site_Count")
      finalDF

    }
  } catch {
    case e: Exception          => { println("Common Exception Occured:" + e); null }
    case _: ClassCastException => { println("some other exception occured"); null }
  }

}