package com.workflow.mapexamples
/*
 * @author Bharath
 */
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
//import org.apache.spark.sql.Dataset
import java.util.ArrayList
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import java.sql.Timestamp
import org.apache.spark.sql.Row
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

object opptyStageCountCalculator {
  /*val testfile = "C:/Users/nookabh/Desktop/json/Opp_History_Staging.csv";
  val jobName = "OptyHistory"
  val conf = new SparkConf().setMaster("local[2]").setAppName(jobName)
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  val sparkSession = SparkSession.builder
    .appName("spark session example").master("local[2]").appName(jobName)
    .config("spark.sql.warehouse.dir", "file:///C:/Users/nookabh/Desktop/json")
    .getOrCreate()
  val spark = SparkSession.builder.master("local[4]").appName("Oppertunity Analysis")
    .getOrCreate()
  def main(args: Array[String]): Unit = {

    case class OppertunityBody(createdDate: String, id: String, oppertunityId: String, stageName: String, systemModSytamp: String)
    import spark.implicits._
    //    val oppertunities_row_df = spark.read.csv(testfile)
    val oppertunities_row_df = sparkSession.read.option("header", "true").csv(testfile)

    //Encoder for Oppertunity Body
    implicit val oppertunityEncoder = org.apache.spark.sql.Encoders.kryo[OppertunityBody] //serializer

    //1. Map by everything to Oppertunity Body Object
    val oppertunityObj_ds: Dataset[OppertunityBody] =
      oppertunities_row_df.
        map(row => OppertunityBody(row.apply(0) + "", row.apply(1) + "", row.apply(2) + "", row.apply(3) + "", row.apply(4) + ""))

    //2. Map by oppId and group them by stage name 
    val oppertunityIdGroups = oppertunityObj_ds.map(oppertunity => {
      (oppertunity.oppertunityId, oppertunity.stageName, 1)
    }).groupByKey(tuple => tuple._1)
    oppertunityIdGroups.count().show()
    println(oppertunityIdGroups.count().show())

    //3. Count the number of times the Oppertunity stayed in a particular stage
    //ex: (0060P00000aLjYWQA0,(0060P00000aLjYWQA0,5 Closed Won,1))
    oppertunityIdGroups.reduceGroups(
      (elem1, elem2) => {
        if (elem1._2 == elem2._2) (elem1._1, elem1._2, elem1._3 + 1)
        else elem1
      }).foreach { x => println(x) }
    //    print(oppertunityIdGroups.count().show())

  }*/
}
