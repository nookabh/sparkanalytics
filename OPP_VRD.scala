package com.workflow.cx

/* @author Bharath
 * 
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

class OPP_VRD(base_df: DataFrame) extends CXTransformationBase {
  try {
    val head = "\"SFDC_OPPORTUNITY_ID__C\",\"VRDQoteCount\"" + System.lineSeparator
    this.writeFile(calculateOppVRD, "OPP_VRD",head)
    def calculateOppVRD(): DataFrame = {
      val parsedf = base_df.groupBy("short_oppty_id").agg(count("Quote_id") as "count")
      val result = parsedf.filter(parsedf("short_oppty_id") !== "").withColumn("SFDCOppId", parsedf("short_oppty_id")).withColumn("VRDQoteCount", parsedf("count")).drop("short_oppty_id").drop("count")
      result
    }
  } catch {
    case e: Exception          => { println("Common Exception Occured:" + e); null }
    case _: ClassCastException => { println("some other exception occured"); null }
  }
}