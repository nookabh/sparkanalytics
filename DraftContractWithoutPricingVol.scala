package com.workflow.cx

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import com.workflow.rt.UtilityClass
import org.apache.spark.sql.functions._

class DraftContractWithoutPricingVol(optyAcc_Df: DataFrame, base_df: DataFrame) extends CXTransformationBase {
  /* @author Bharath
   	Metric : Draft Without Pricing Volume  Metric
 	*/
  try {
    val head = "\"SFDCOppId\",\"EventName\",\"EventType\",\"Count\"" + System.lineSeparator
    this.writeFile(calculateVPApprovalVolume, "DRAFT_WITHOUT_PRICING_VOL", head)
    def calculateVPApprovalVolume(): DataFrame = {
      val based_result = base_df.filter(base_df("MILESTONE_ID") === "10601012").groupBy("short_oppty_id").count()
      val optyAcc = optyAcc_Df.filter(optyAcc_Df("STAGENAME") === "2 Solve" || optyAcc_Df("STAGENAME") === "3 Propose" || optyAcc_Df("STAGENAME") === "4 Negotiate" || optyAcc_Df("STAGENAME") === "5 Closed Won")
        .select(optyAcc_Df("SFDC_OPPORTUNITY_ID__C"))
      var final_df = based_result.join(optyAcc, optyAcc("SFDC_OPPORTUNITY_ID__C") === based_result("short_oppty_id")).drop(col("SFDC_OPPORTUNITY_ID__C")).withColumn("EventType", lit("ROLLING"))
        .withColumn("EventName", lit("DRAFT_WITHOUT_PRICING_VOL"))
      final_df = final_df.select(col("short_oppty_id"), col("EventName"), col("EventType"), col("count"))
      final_df
    }
  } catch {
    case e: Exception          => { println("Common Exception Occured:" + e); null }
    case _: ClassCastException => { println("some other exception occured"); null }
  }
}
