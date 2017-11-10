package com.workflow.cx

/* @author bnookala
 * SalesHierarchyMetric
 * 
 */

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.udf

import com.workflow.rt.UtilityClass

class SalesHierarchyMetric(base_df: DataFrame, user_df: DataFrame, sales_dir: DataFrame, sqlContext: org.apache.spark.sql.SQLContext) extends CXTransformationBase {
  try {
    val head = "\"SFDCOppId\",\"OWNERID\",\"ALLNASP_DIR_NAME__C\", \"ZVP_USER__C\"" + System.lineSeparator
    val basedf = base_df.join(user_df, user_df("ID") === base_df("OWNERID")).withColumn("VZ_ID", user_df("VZ_ID__C"))
    basedf.registerTempTable("NEW_USER_Table");
    var Eid_data =sqlContext.sql("SELECT ENTERPRISE_ID__C,VZ_ID,SFDC_OPPORTUNITY_ID__C, OWNERID,EID_8__C, EID_7__C, EID_6__C, EID_5__C, EID_4__C, EID_3__C, EID_2__C, EID_1__C, EID_0__C from NEW_USER_Table")
    var dataframe = Eid_data.withColumn("EidFilterType", concat(col("EID_8__C"),lit("_"),col("EID_7__C"),col("EID_6__C"),lit("_"),col("EID_5__C"),col("EID_4__C"),lit("_"),col("EID_3__C"),col("EID_2__C"),lit("_"),col("EID_1__C"),lit("_"),col("EID_0__C")))
    var salesdataframe = dataframe.join(sales_dir, sales_dir("VZ_ID__C") === dataframe("VZ_ID")).withColumn("Owner_Id", dataframe("OWNERID"))
    val resultset = salesdataframe.select("SFDC_OPPORTUNITY_ID__C", "Owner_Id", "ALLNASP_DIR_NAME__C", "ZVP_USER__C")
    this.writeFile(resultset, "SALES_HIERARCHY", head)
  } catch {
    case e: Exception          => { println("Common Exception Occured:" + e); null }
    case _: ClassCastException => { println("some other exception occured"); null }
  }
}
