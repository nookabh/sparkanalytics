package com.workflow.cx

/* @author cholaan
   	Metric : Contract Initiated To Signed
   	Max of Contract Signed Event Received - Min of Opportunity Created Date
   		Opportunity Created Date
			10601020 - Contract Signed Event Received
   	 475
 	*/

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import com.workflow.rt.UtilityClass
import org.apache.spark.sql.functions._

class ContractSign(base_df: DataFrame) extends CXTransformationBase {
  try {
    val head = "\"SFDCOppId\",\"EventName\",\"EventType\",\"EventStartDate\",\"EventCompletionDate\",\"Count\"" + System.lineSeparator
    this.writeFile(calculateContractSign, "OPP_CONTRACT_SIGN", head)
    def calculateContractSign(): DataFrame = {
      val findDiffUDF = udf(UtilityClass.minus _)
      val timeToStrUDF = udf(UtilityClass.timeToStr _)
      val fullDateToMillSecondUDF = udf(UtilityClass.fullDateToMillSecond _)
      val changeMSToDay = udf(UtilityClass.millisecondToDay _)
      val removeComma = udf(UtilityClass.removeCommaFormatter _)
      val created_df = base_df.withColumn("startDateMS", fullDateToMillSecondUDF(base_df("CREATEDDATE")))
        .withColumn("signedDateMS", fullDateToMillSecondUDF(base_df("CREATEDDATE")))
      val createdMin_df = created_df.groupBy("SFDC_OPPORTUNITY_ID__C").agg(min("startDateMS") as "CreatedDateMS")
      val signedMax_df = created_df.groupBy("SFDC_OPPORTUNITY_ID__C").agg(max("signedDateMS") as "SignedDateMS")
      val getMaxClosedDF = signedMax_df.join(created_df, "SFDC_OPPORTUNITY_ID__C")
        .where(created_df("SFDC_OPPORTUNITY_ID__C") === signedMax_df("SFDC_OPPORTUNITY_ID__C") and created_df("signedDateMS") === signedMax_df("SignedDateMS"))
      val signed_df = getMaxClosedDF.filter(getMaxClosedDF("STAGENAME") === "5 Closed Won")
      val join_df = createdMin_df.join(signed_df, "SFDC_OPPORTUNITY_ID__C")
      val result_df = join_df.withColumn("CountMS", findDiffUDF(join_df("SignedDateMS"), join_df("CreatedDateMS")))
      val final_df = result_df.withColumn("Count", changeMSToDay(result_df("CountMS")))
        .withColumn("SFDCOppId", result_df("SFDC_OPPORTUNITY_ID__C"))
        .withColumn("EventStartDate", timeToStrUDF(result_df("CreatedDateMS")))
        .withColumn("EventCompletionDate", timeToStrUDF(result_df("SignedDateMS")))
        .withColumn("EventName", lit("OPP_CONTRACT_SIGN"))
        .withColumn("EventType", lit("ROLLING"))
      val return_df = final_df.select("SFDCOppId", "EventName", "EventType", "EventStartDate", "EventCompletionDate", "Count")
      return_df
    }
  } catch {
    case e: Exception          => { println("Common Exception Occured:" + e); null }
    case _: ClassCastException => { println("some other exception occured"); null }
  }
}
