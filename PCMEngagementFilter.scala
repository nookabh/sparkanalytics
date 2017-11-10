
/* https://onejira.verizon.com/browse/NVDW-455
Author boggusi */

package com.workflow.cx
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

class PCMEngagementFilter(opp_hist_df: DataFrame, teamform_hist_df: DataFrame, quote_df: DataFrame) extends CXTransformationBase {
  try {
    val head = "\"SFDCOppId\",\"PCN_engaged\"" + System.lineSeparator
    val pcm_team = List("USPCM","SSPCM-HOST","SSPCM-IPAH","SSPCM-COLO","LATAM PCM","APAC PCM","EMEA PCM","SSPCM-PS","SSPCM-RAM","SSPCM-IPAH","SSPCM-VIT","SSPCM-UCC","SSPCM-SEC","SSPCM-COLO","SSPCM-HOST","SSPCMCloud")
    this.writeFile(calculatePCMEngagementFilterMetric, "PCM_FILTER", head)
    def calculatePCMEngagementFilterMetric(): DataFrame = {
      
       var unique_opp_df = opp_hist_df.select("SFDC_OPPORTUNITY_ID__C").distinct()
       var sfdc_pcm_df = teamform_hist_df.filter(teamform_hist_df("TEAM_INITIALS__C").isin(pcm_team:_*))
                                         .select("SFDC_OPPORTUNITY_ID__C").distinct()
                                         .withColumn("PCM_engaged", lit("YES"))
       
       
       
       var PCM_engaged_df = unique_opp_df.join(sfdc_pcm_df, Seq("SFDC_OPPORTUNITY_ID__C"), "left_outer")
       PCM_engaged_df
    }
  } catch {
    case e: Exception          => { println("Common Exception Occured:" + e); null }
    case _: ClassCastException => { println("some other exception occured"); null }
  }
}