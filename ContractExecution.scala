package com.workflow.cx

/* @author cholaan
   		Metric : Opportunity ContractCompleted   
 	*/

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import com.workflow.rt.UtilityClass
import org.apache.spark.sql.functions._

class ContractExecution(gct_df: DataFrame, pq_df: DataFrame) extends CXTransformationBase {

  try {

    val head = "\"SFDCOppId\",\"EventName\",\"EventType\",\"EventStartDate\",\"EventCompletionDate\",\"Count\"" + System.lineSeparator

    this.writeFile(calculateContractExecution, "CONTRACT_EXECUTION", head)
    def calculateContractExecution(): DataFrame = {
      val dateToMillSecondUDF = udf(UtilityClass.dateToMillSecond _)
      val convertTimeForGCTUDF = udf(UtilityClass.timeDiffForGCT _)
      val oppIdDF = pq_df.groupBy(pq_df("short_oppty_id"), pq_df("Quote_id")).count()
      val quoteFilterDF = gct_df.filter(gct_df("MODIFICATION") === "Amendment Status updated to CD-Approved" || gct_df("MODIFICATION") === "Amendment Status updated to Downloaded")
      val quoteAssMinDF = quoteFilterDF.groupBy("QUOTE_ID").agg(min("MODIFICATION_DATE") as "Min_MODIFICATION_DATE", max("QUOTE_VERSION_ID") as "Version_id")
      val quoteStatusMaxDF = quoteFilterDF.groupBy("QUOTE_ID").agg(max("MODIFICATION_DATE") as "Max_MODIFICATION_DATE", max("QUOTE_VERSION_ID") as "Version_id")
      val joinDF = quoteStatusMaxDF.join(quoteAssMinDF, "QUOTE_ID")
      val resultDF = joinDF.withColumn("Diff", convertTimeForGCTUDF(joinDF("Min_MODIFICATION_DATE"), joinDF("Max_MODIFICATION_DATE")))
      val outDF = resultDF.join(oppIdDF, oppIdDF("Quote_id") === resultDF("QUOTE_ID"))
        .withColumn("EventName", lit("CONTRACT_EXECUTION"))
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
