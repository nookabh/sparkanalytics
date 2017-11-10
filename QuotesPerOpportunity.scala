package com.workflow.cx
/* @author cholaan
   	Metric : Quote per Opportunity
   	JIRA:NVDW-510
 	*/
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import com.workflow.rt.UtilityClass
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext

class QuotesPerOpportunity(pqDF: DataFrame) extends CXTransformationBase {
  try {

    val head = "\"SFDCOppId\",\"EventName\",\"EventType\",\"EventStartDate\",\"EventCompletionDate\",\"Count\"" + System.lineSeparator
    this.writeFile(calculateSitePerMultiQuoteSet, "NO_OF_QUOTES", head)
    def calculateSitePerMultiQuoteSet(): DataFrame = {
     
      val quoteCountDF = pqDF.groupBy("short_oppty_id").agg(countDistinct("Quote_id") as "Count",first("milestone_date").substr(0, 10) as "EventStartDate")
      .withColumn("EventCompletionDate", lit(""))
      .withColumn("EventName", lit("NO_OF_QUOTES"))
       .withColumn("EventType", lit("ROLLING"))
    val finalDF = quoteCountDF.select("short_oppty_id", "EventName", "EventType", "EventStartDate", "EventCompletionDate", "Count")
    // finalDF.show(100)   
    finalDF
     
    }
  } catch {
    case e: Exception          => { println("Common Exception Occured:" + e); null }
    case _: ClassCastException => { println("some other exception occured"); null }
  }

}