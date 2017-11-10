package com.workflow.cx
/* @author Siva
 * https://onejira.verizon.com/browse/NVDW-481
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

class Stage2toVIDEDesignStart (OppHist_df: DataFrame, ViseHist_df: DataFrame) extends CXTransformationBase {
  try {
    val head = "\"SFDCOppId\",\"EventName\",\"EventType\",\"EventStartDate\",\"EventCompletionDate\",\"Count\"" + System.lineSeparator  
    this.writeFile(calculateStage2toVIDEDesignStartMetric, "STAGE2_VISE", head)
    def calculateStage2toVIDEDesignStartMetric(): DataFrame = {
      val dateToMillSecondUDF = udf(UtilityClass.fullDateToMillSecond _)
      val milliSecToDays = udf(com.workflow.rt.UtilityClass.millisecondToDay _)
      var base_df = OppHist_df
      base_df = base_df.drop(base_df.col("OPPTY_Hist_ID"))
      base_df = base_df.drop(base_df.col("SYSTEMMODSTAMP"))
      base_df = base_df.withColumn("timestampsec", dateToMillSecondUDF(base_df("CREATEDDATE")))
      var stage2_df = base_df.filter("STAGENAME = '2 Solve'")
                             .groupBy("SFDC_OPPORTUNITY_ID__C")
                             .agg(max("timestampsec"))
                             .withColumnRenamed("max(timestampsec)", "timestamp_stage2")
      
      var Vise_hist_df = ViseHist_df.withColumn("timestampsec", dateToMillSecondUDF(base_df("VISE_CREATED_DATE"))) 
      Vise_hist_df = Vise_hist_df.groupBy("SFDC_OPPORTUNITY_ID__C")
                                 .agg(min("timestampsec"))
                                 .withColumnRenamed("max(timestampsec)", "timestamp_vise")
      var stage2tovise_df = stage2_df.join(Vise_hist_df,"SFDC_OPPORTUNITY_ID__C")
      stage2tovise_df = stage2tovise_df.withColumn("stage2tovise_duration", stage2tovise_df("timestamp_vise") - stage2tovise_df("timestamp_stage2"))
                                       .withColumn("Count", milliSecToDays(stage2tovise_df("stage2tovise_duration")))
                                       .withColumn("EventType", lit("ROLLING"))
                                       .withColumn("EventName", lit("DESIGN_VISESTART"))
                                       .withColumn("EventStartDate", milliSecToDays(stage2tovise_df("timestamp_stage2")))
                                       .withColumn("EventCompletionDate", milliSecToDays(stage2tovise_df("timestamp_vise")))
                                       .withColumn("SFDCOppId", stage2tovise_df("SFDC_OPPORTUNITY_ID__C"))
      var stage2toviseOut_df = stage2tovise_df.select("SFDCOppId", "EventName", "EventType", "EventStartDate", "EventCompletionDate", "Count")
      stage2toviseOut_df                                 
      
        }  
      } catch {
    case e: Exception          => { println("Common Exception Occured:" + e); null }
    case _: ClassCastException => { println("some other exception occured"); null }
  }
}