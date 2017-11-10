package com.workflow.cx

/* @author girish - GSATMetric
   * 
   */

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import com.workflow.rt.UtilityClass
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.min
import org.apache.spark.sql.functions.udf
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.functions._
import java.util.Calendar

class SupportResourcesFilter(Oppty_DF: DataFrame,GSAT_df: DataFrame,DA_DF:DataFrame,VISE_DF:DataFrame) extends CXTransformationBase
{
  try{
      val head = "\"SFDCOppId\",\"GSAT\",\"DA\",\"VISE\"" + System.lineSeparator
    this.writeFile(SupportResourcesFilter, "SupportResources_FILTER", head)
    def SupportResourcesFilter(): DataFrame = 
    {
      var CompOpptydf= Oppty_DF.filter(Oppty_DF("Oppty_id")!=="Oppty_id").select(col("SFDC_OPPORTUNITY_ID__C")).distinct()
      var compGSATdf  = GSAT_df.filter(GSAT_df("ID") !== "ID").select(col("OPPORTUNITY_ID_SHORT__C")).withColumn("GSAT", lit("1")).distinct()
      var compDAdf = DA_DF.filter(DA_DF("ID")!=="ID").select(col("SFDC_OPPORTUNITY_ID__C")).withColumn("DA", lit("1")).distinct()
      var compVISEdf = VISE_DF.filter(VISE_DF("ID")!=="ID").select(col("SFDC_OPPORTUNITY_ID__C")).withColumn("VISE", lit("1")).distinct() 
      var resultdf = CompOpptydf.join(compGSATdf, CompOpptydf("SFDC_OPPORTUNITY_ID__C")===compGSATdf("OPPORTUNITY_ID_SHORT__C"),"left_outer")
      resultdf = resultdf.drop(resultdf.col("OPPORTUNITY_ID_SHORT__C")).withColumn("Opptyshortid", col("SFDC_OPPORTUNITY_ID__C"))
      resultdf = resultdf.drop(resultdf.col("SFDC_OPPORTUNITY_ID__C"))
      var finalresultdf = resultdf.join(compDAdf, resultdf("Opptyshortid")===compDAdf("SFDC_OPPORTUNITY_ID__C"),"left_outer")
      finalresultdf = finalresultdf.drop(finalresultdf.col("SFDC_OPPORTUNITY_ID__C")).select(col("Opptyshortid"),col("GSAT"),col("DA"))
      
       var COMPresultdf = finalresultdf.join(compVISEdf, compVISEdf("SFDC_OPPORTUNITY_ID__C")===finalresultdf("Opptyshortid"),"left_outer")
      COMPresultdf = COMPresultdf.drop(col("SFDC_OPPORTUNITY_ID__C")).select(col("Opptyshortid"),col("GSAT"),col("DA"),col("VISE"))
      //COMPresultdf.where(col("GSAT").equalTo("1") || col("DA").equalTo("1") || col("VISE").equalTo("1") ).sort("Opptyshortid").show(100)
      COMPresultdf
    }
  } catch {
    case e: Exception          => { println("Common Exception Occured:" + e); null }
    case _: ClassCastException => { println("some other exception occured"); null }
  }
}
