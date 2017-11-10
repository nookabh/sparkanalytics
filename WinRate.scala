package com.workflow.cx

/* @author Siva
 * 
 */

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.lag
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
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.hive.HiveContext

class WinRate(base_dataframe: DataFrame) extends CXTransformationBase {
  try {
    val head = "\"SFDCOppId\",\"EventName\",\"EventType\",\"EventStartDate\",\"EventCompletionDate\",\"Count\"" + System.lineSeparator
    this.writeFile(calculateWinRateMetric, "STATUS_ALL", head)
    def calculateWinRateMetric(): DataFrame = {
      var base_df = base_dataframe.drop(base_dataframe.col("OPPTY_Hist_ID"))
      base_df = base_df.drop(base_df.col("SYSTEMMODSTAMP"))
      def timeToStr = udf((milliSec: Long) => {
        DateTimeFormat.forPattern("MM/dd/YYYY").print(milliSec)
      })
      def udfToTime = udf((timeStamp: String) => {
        if (timeStamp == null || timeStamp == "CREATEDDATE" || timeStamp.isEmpty()) {
          0
        } else {
          val inputFormat = new SimpleDateFormat("MM/dd/yyyy' 'HH:mm:ss")
          var date = inputFormat.parse(timeStamp)
          date.getTime()
        }
      })
      base_df = base_df.withColumn("timestampsec", udfToTime(base_df("CREATEDDATE")))
                      // .withColumn("Prev_Opp", lag(base_df("SFDC_OPPORTUNITY_ID__C"),1))
                       .withColumn("Prev_Stage", lag("STAGENAME", 1).over(Window.partitionBy("SFDC_OPPORTUNITY_ID__C").orderBy("timestampsec")))
      base_df = base_df.na.fill("")
      base_df = base_df.filter("STAGENAME != Prev_Stage")
      var OppMax_df = base_df.groupBy("SFDC_OPPORTUNITY_ID__C").agg(max("timestampsec"))
      OppMax_df = OppMax_df.withColumnRenamed("SFDC_OPPORTUNITY_ID__C", "SFDC_OPPORTUNITY_ID")
      var OppStage_df = base_df.join(
        OppMax_df,
        base_df("SFDC_OPPORTUNITY_ID__C") === OppMax_df("SFDC_OPPORTUNITY_ID")
          && base_df("timestampsec") === OppMax_df("max(timestampsec)"))
      var run_date = "01/01/2016"
      val date_format = new SimpleDateFormat("MM/dd/yyyy")
      var run_date_sec = date_format.parse(run_date).getTime()
      OppStage_df = OppStage_df.filter(OppStage_df.col("max(timestampsec)") > run_date_sec)
      def statustext = udf((inStatus: String) => {
        inStatus match {
          case "0 Identify"            => "IDENTIFY"
          case "1 Qualify"             => "QUALIFY"
          case "2 Solve"               => "SOLVE"
          case "3 Propose"             => "PROPOSE"
          case "4 Negotiate"           => "NEGOTIATE"
          case "5 Closed Lost"         => "CLOSED_LOST"
          case "5 Closed Won"          => "CLOSED_WON"
          case "5 Closed Disqualified" => "CLOSED_DISQUALIFIED"
          case _                       => "Unknown"
        }
      })
      OppStage_df = OppStage_df
        .withColumn("EventType", lit("ROLLING"))
        .withColumn("EventName", statustext(OppStage_df("STAGENAME")))
        .withColumn("EventStartDate", lit(""))
        .withColumn("EventCompletionDate", timeToStr(OppStage_df("max(timestampsec)")))
        .withColumn("Count", lit(1))
        .withColumn("SFDCOppId", OppStage_df("SFDC_OPPORTUNITY_ID__C"))
      var OppStageOut_df = OppStage_df.select("SFDCOppId", "EventName", "EventType", "EventStartDate", "EventCompletionDate", "Count")
      OppStageOut_df
    }
  } catch {
    case e: Exception          => { println("Common Exception Occured:" + e); null }
    case _: ClassCastException => { println("some other exception occured"); null }
  }
}
