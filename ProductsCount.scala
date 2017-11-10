package com.workflow.cx
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import com.workflow.rt.UtilityClass
import org.apache.spark.sql.functions._

class ProductsCount(base_df: DataFrame) extends CXTransformationBase {
  try {
    val head = "\"SFDCOppId\",\"Count\"" + System.lineSeparator
    this.writeFile(calculateProductCount, "PRODUCT_COUNT", head)
    def calculateProductCount(): DataFrame = {
      val prod_df = base_df.groupBy("SFDC_OPPORTUNITY_ID__C").count()
      prod_df 
    }
  } catch {
    case e: Exception          => { println("Common Exception Occured:" + e); null }
    case _: ClassCastException => { println("some other exception occured"); null }
  }
}
