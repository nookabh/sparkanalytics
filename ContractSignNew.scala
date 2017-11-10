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

class ContractSignNew(base_df: DataFrame) extends CXTransformationBase {
  try {
    val head = "\"SFDCOppId\",\"EventName\",\"EventType\",\"EventStartDate\",\"EventCompletionDate\",\"Count\"" + System.lineSeparator
    this.writeFile(calculateContractSign, "OPP_CONTRACT_SIGN", head)
    def calculateContractSign(): DataFrame = {
      val minusUDF = udf(UtilityClass.minus _)
      val timeToStrUDF = udf(UtilityClass.timeToStr _)
      val dateToMillSecondUDF = udf(UtilityClass.dateToMillSecond _)
      val changeMSToDay = udf(UtilityClass.millisecondToDay _)
      val removeComma = udf(UtilityClass.removeCommaFormatter _)
      val created_df = base_df.withColumn("startDateMS", dateToMillSecondUDF(base_df("Oppty_create_Date")))
        .withColumn("milestoneId", removeComma(base_df("MILESTONE_ID")))
        .withColumn("signedDateMS", dateToMillSecondUDF(base_df("milestone_date")))
      val signed_df = created_df.filter((created_df("milestoneId") === "10601020"))

      val createdMin_df = created_df.groupBy("short_oppty_id").agg(min("startDateMS") as "CreatedDateMS")
      val signedMax_df = signed_df.groupBy("short_oppty_id").agg(max("signedDateMS") as "SignedDateMS")
      val join_df = createdMin_df.join(signedMax_df, "short_oppty_id")
      val result_df = join_df.withColumn("CountMS", minusUDF(join_df("SignedDateMS"), join_df("CreatedDateMS")))
      val final_df = result_df.withColumn("Count", changeMSToDay(result_df("CountMS")))
        .withColumn("SFDCOppId", result_df("short_oppty_id"))
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
