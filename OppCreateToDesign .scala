package com.workflow.cx
/* @author Siva
 * https://onejira.verizon.com/browse/NVDW-476
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

class OppCreateToDesign(base_dataframe: DataFrame) extends CXTransformationBase {
  try {
    val head = "\"SFDCOppId\",\"EventName\",\"EventType\",\"EventStartDate\",\"EventCompletionDate\",\"Count\"" + System.lineSeparator
    this.writeFile(calculateOppCreateToDesignMetric, "OPP_DESIGN", head)
    def calculateOppCreateToDesignMetric(): DataFrame = {
      var base_df = base_dataframe
      base_df = base_df.drop(base_df.col("OPPTY_Hist_ID"))
      base_df = base_df.drop(base_df.col("SYSTEMMODSTAMP"))

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
      base_df = base_df.withColumn("timestampsec", udfToTime(base_df("CREATEDDATE")))
      var created_df = base_df.groupBy("SFDC_OPPORTUNITY_ID__C").agg(min("timestampsec"))
      var stage2_df = base_df.filter("STAGENAME = '2 Solve'").groupBy("SFDC_OPPORTUNITY_ID__C").agg(max("timestampsec"))
      var stage2duration_df = created_df.join(stage2_df, "SFDC_OPPORTUNITY_ID__C")
      stage2duration_df = stage2duration_df.withColumn("Stage2duration", stage2duration_df("max(timestampsec)") - stage2duration_df("min(timestampsec)"))
      stage2duration_df = stage2duration_df
        .withColumn("EventType", lit("ROLLING"))
        .withColumn("EventName", lit("OPP_DESIGN"))
        .withColumn("EventStartDate", timeToStr(stage2duration_df("min(timestampsec)")))
        .withColumn("EventCompletionDate", timeToStr(stage2duration_df("max(timestampsec)")))
        .withColumn("Count", secToDays(stage2duration_df("Stage2duration")))
        .withColumn("SFDCOppId", stage2duration_df("SFDC_OPPORTUNITY_ID__C"))
      val stage2Out_df = stage2duration_df.select("SFDCOppId", "EventName", "EventType", "EventStartDate", "EventCompletionDate", "Count")
      stage2Out_df
    }
  } catch {
    case e: Exception          => { println("Common Exception Occured:" + e); null }
    case _: ClassCastException => { println("some other exception occured"); null }
  }
}