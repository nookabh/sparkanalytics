package com.workflow.cx
/* @author Siva
 * https://onejira.verizon.com/browse/NVDW-
 */

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import com.workflow.rt.UtilityClass
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.min
import org.apache.spark.sql.functions.udf
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.functions._
import java.util.Calendar

class OppDesignToQuote(base_dataframe: DataFrame, base_quote_df: DataFrame) extends CXTransformationBase {
  try {
    val head = "\"SFDCOppId\",\"EventName\",\"EventType\",\"EventStartDate\",\"EventCompletionDate\",\"Count\"" + System.lineSeparator  
    this.writeFile(calculateOppDesignToQuoteMetric, "DESIGN_QUOTE", head)
    def calculateOppDesignToQuoteMetric(): DataFrame = {

      def udfToTime = udf((timeStamp: String) => {
        if (timeStamp == null || timeStamp == "CREATEDDATE" || timeStamp.isEmpty()) {
          0
        } else {
          val inputFormat = new SimpleDateFormat("MM/dd/yyyy' 'HH:mm:ss")
          var date = inputFormat.parse(timeStamp)
          date.getTime()
        }
      })

      def timeToStr = udf((milliSec: Long) => {
        DateTimeFormat.forPattern("MM/dd/YYYY").print(milliSec)
      })
      val secToDays = udf(com.workflow.rt.UtilityClass.millisecondToDay _)
      def udfToTime_quote = udf((timeStamp: String) => {
        //val inputFormat = new SimpleDateFormat("M/d/yyyy h:mm:ss a")  
        val inputFormat = new SimpleDateFormat("M/d/yyyy")
        var date = inputFormat.parse(timeStamp)
        date.getTime()
      })
      var base_df = base_dataframe
      base_df = base_df.drop(base_df.col("OPPTY_Hist_ID"))
      base_df = base_df.drop(base_df.col("SYSTEMMODSTAMP"))
      base_df = base_df.withColumn("timestampsec", udfToTime(base_df("CREATEDDATE")))
      var created_df = base_df.groupBy("SFDC_OPPORTUNITY_ID__C").agg(min("timestampsec"))
      var stage2_df = base_df.filter("STAGENAME = '2 Solve'").groupBy("SFDC_OPPORTUNITY_ID__C").agg(max("timestampsec"))
      var stage2duration_df = created_df.join(stage2_df, "SFDC_OPPORTUNITY_ID__C")
      var quote_base_df = base_quote_df
      // Removing unwanted data
      quote_base_df = quote_base_df.drop(quote_base_df.col("Quote_version"))
      quote_base_df = quote_base_df.drop(quote_base_df.col("PP_Quote_Type"))
      quote_base_df = quote_base_df.drop(quote_base_df.col("linked_quote_create_date"))
      quote_base_df = quote_base_df.drop(quote_base_df.col("base_quote_id"))
      quote_base_df = quote_base_df.drop(quote_base_df.col("Oppty_create_Date"))
      quote_base_df = quote_base_df.drop(quote_base_df.col("OPPORTUNITY_NAME"))
      quote_base_df = quote_base_df.drop(quote_base_df.col("ACCOUNT_NAME"))
      quote_base_df = quote_base_df.drop(quote_base_df.col("MILESTONE_DESC"))
      quote_base_df = quote_base_df.filter("MILESTONE_ID in ('10001010', '10001088', '10001410')")
      quote_base_df = quote_base_df.withColumn("milestone_sec", udfToTime_quote(quote_base_df("milestone_date")))
      var quote_created_df = quote_base_df.groupBy("short_oppty_id").agg(min("milestone_sec"))
      var quotetostage2_df = stage2_df.join(quote_created_df, stage2_df("SFDC_OPPORTUNITY_ID__C") === quote_created_df("short_oppty_id"))
      quotetostage2_df = quotetostage2_df.withColumn("quotetostage2_dur", quotetostage2_df("max(timestampsec)") - quotetostage2_df("min(milestone_sec)"))
      quotetostage2_df = quotetostage2_df
        .withColumn("EventType", lit("ROLLING"))
        .withColumn("EventName", lit("DESIGN_QUOTE"))
        .withColumn("EventStartDate", timeToStr(quotetostage2_df("min(milestone_sec)")))
        .withColumn("EventCompletionDate", timeToStr(quotetostage2_df("max(timestampsec)")))
        .withColumn("Count", secToDays(quotetostage2_df("quotetostage2_dur")))
        .withColumn("SFDCOppId", quotetostage2_df("SFDC_OPPORTUNITY_ID__C"))
      var quotetostage2Out_df = quotetostage2_df.select("SFDCOppId", "EventName", "EventType", "EventStartDate", "EventCompletionDate", "Count")
      quotetostage2Out_df
    }
  } catch {
    case e: Exception          => { println("Common Exception Occured:" + e); null }
    case _: ClassCastException => { println("some other exception occured"); null }
  }

}