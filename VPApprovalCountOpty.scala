package com.workflow.cx

/* @author bnookala
   * NVDW-491
    "10251060 - Rate and Term Approval Requested (RTB)
    10301060 - Price Quote Approval Requested (TX)
    
    10251070 - Rate and Term Entitlement Discounts Approved (RTB)
    10301070 - Price Quote Entitlement Discounts Approved (TX)"
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

class VPApprovalCountOpty(base_df: DataFrame) extends CXTransformationBase {
  try {
    val head = "\"SFDCOppId\",\"EventName\",\"EventType\",\"EventStartDate\",\"EventCompletionDate\",\"Count\"" + System.lineSeparator
    this.writeFile(calculateVPApprovalCountOpty, "VP_APPROVAL_CountOpty",head)

    def calculateVPApprovalCountOpty(): DataFrame = {
   val parsedf = base_df.filter(base_df("MILESTONE_ID") === "10301070").groupBy("short_oppty_id").agg(count("Quote_id") as "count")
   parsedf.show()
   val result = parsedf.filter(parsedf("short_oppty_id") !== "").withColumn("SFDCOppId", parsedf("short_oppty_id")).withColumn("VRDQoteCount", parsedf("count")).drop("short_oppty_id").drop("count")
     
      result
    }
  } catch {
    case e: Exception          => { println("Common Exception Occured:" + e); null }
    case _: ClassCastException => { println("some other exception occured"); null }
  }
}
