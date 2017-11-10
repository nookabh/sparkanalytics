package com.workflow.cx

/* @author Siva
 * https://onejira.verizon.com/browse/NVDW-477
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

class OppStage0(base_dataframe: DataFrame) extends CXTransformationBase {
  try {
    val head = "\"SFDCOppId\",\"EventName\",\"EventType\",\"EventStartDate\",\"EventCompletionDate\",\"Count\"" + System.lineSeparator
    this.writeFile(calculateOppStage0Metric, "OPP_STAGE0", head)
    def calculateOppStage0Metric(): DataFrame = {

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
      var base_df = base_dataframe
      base_df = base_df.drop(base_df.col("OPPTY_Hist_ID"))
      base_df = base_df.drop(base_df.col("SYSTEMMODSTAMP"))
      base_df = base_df.withColumn("timestampsec", udfToTime(base_df("CREATEDDATE")))
      //Getting created dates for opportunities
      var created_df = base_df.groupBy("SFDC_OPPORTUNITY_ID__C").agg(min("timestampsec"))
      //Getting stage1 dates for opportunities
      var stage1_df = base_df.filter("STAGENAME = '1 Qualify'").groupBy("SFDC_OPPORTUNITY_ID__C").agg(max("timestampsec"))
      var stage1duration_df = created_df.join(stage1_df, "SFDC_OPPORTUNITY_ID__C")
      stage1duration_df = stage1duration_df.withColumn("Stage1duration", stage1duration_df("max(timestampsec)") - stage1duration_df("min(timestampsec)"))
      stage1duration_df = stage1duration_df
        .withColumn("EventType", lit("ROLLING"))
        .withColumn("EventName", lit("OPP_STAGE0"))
        .withColumn("EventStartDate", timeToStr(stage1duration_df("min(timestampsec)")))
        .withColumn("EventCompletionDate", timeToStr(stage1duration_df("max(timestampsec)")))
        .withColumn("Count", secToDays(stage1duration_df("Stage1duration")))
        .withColumn("SFDCOppId", stage1duration_df("SFDC_OPPORTUNITY_ID__C"))
      var stage1Out_df = stage1duration_df.select("SFDCOppId", "EventName", "EventType", "EventStartDate", "EventCompletionDate", "Count")
      stage1Out_df
    }
  } catch {
    case e: Exception          => { println("Common Exception Occured:" + e); null }
    case _: ClassCastException => { println("some other exception occured"); null }
  }
}