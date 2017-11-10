package com.workflow.cx

/* @author cholaan
   		Metric : Opportunity ContractCompleted   
 	*/

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import com.workflow.rt.UtilityClass
import org.apache.spark.sql.functions._

class OPPContractCompleted(base_df: DataFrame) extends CXTransformationBase {
  try {
    val head = "\"SFDCOppId\",\"IsContractCompleted\"" + System.lineSeparator
    this.writeFile(calculateOPPContractCompleted, "OPP_CONTRACT_COMPLETED",head)   
    def calculateOPPContractCompleted(): DataFrame = {
      val signed_df = base_df.filter((base_df("MILESTONE_ID") === "10601020"))
      val createdMin_df = signed_df.groupBy("short_oppty_id").count()
      val final_df = createdMin_df
        .withColumn("SFDCOppId", createdMin_df("short_oppty_id"))
        .withColumn("IsContractCompleted", lit("TRUE"))
      val return_df = final_df.select("SFDCOppId", "IsContractCompleted")
      return_df
    }
  } catch {
    case e: Exception          => { println("Common Exception Occured:" + e); null }
    case _: ClassCastException => { println("some other exception occured"); null }
  }
}
