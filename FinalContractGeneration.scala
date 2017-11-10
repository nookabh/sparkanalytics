package com.workflow.cx

/* @author cholaan
   		Metric : FINAL_CONTRACT_GENERATION
 	*/

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import com.workflow.rt.UtilityClass
import org.apache.spark.sql.functions._

class FinalContractGeneration(gct_df: DataFrame, pq_df: DataFrame) extends CXTransformationBase {

  try {
    val head = "\"SFDCOppId\",\"EventName\",\"EventType\",\"EventStartDate\",\"EventCompletionDate\",\"Count\"" + System.lineSeparator
    this.writeFile(calculateContractExecution, "FINAL_CONTRACT_GENERATION", head)
    def calculateContractExecution(): DataFrame = {
      val dateToMillSecondUDF = udf(UtilityClass.dateToMillSecond _)
      val convertTimeForGCTUDF = udf(UtilityClass.timeDiffForGCT _)
      val oppIdDF = pq_df.groupBy(pq_df("short_oppty_id"), pq_df("Quote_id")).count()
      val quoteAssDF = gct_df.filter(gct_df("QUOTE_VERSION_ID") === 0 && gct_df("EVENT") === "Quote-Association")
        .groupBy("QUOTE_ID").agg(min("MODIFICATION_DATE") as "Min_MODIFICATION_DATE")
      val quoteStatusDF = gct_df.filter(gct_df("EVENT") === "Status Update" && gct_df("MODIFICATION") === "Amendment Status updated to Downloaded")
        .groupBy("QUOTE_ID").agg(max("MODIFICATION_DATE") as "Max_MODIFICATION_DATE")
      val joinDF = quoteAssDF.join(quoteStatusDF, "QUOTE_ID")
      val resultDF = joinDF.withColumn("Diff", convertTimeForGCTUDF(joinDF("Min_MODIFICATION_DATE"), joinDF("Max_MODIFICATION_DATE")))
      val outDF = resultDF.join(oppIdDF, oppIdDF("Quote_id") === resultDF("QUOTE_ID"))
        .withColumn("EventName", lit("FINAL_CONTRACT_GENERATION"))
        .withColumn("EventType", lit("ROLLING"))
      val finalDF = outDF.select("short_oppty_id", "EventName", "EventType", "Min_MODIFICATION_DATE", "Max_MODIFICATION_DATE", "Diff")
      //finalDF.show()
      finalDF

    }
  } catch {
    case e: Exception          => { println("Common Exception Occured:" + e); null }
    case _: ClassCastException => { println("some other exception occured"); null }
  }
}
