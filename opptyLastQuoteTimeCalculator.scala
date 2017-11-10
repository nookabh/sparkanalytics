package com.workflow.mapexamples
/*
 * @author Bharath
 */
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.ArrayList
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import java.sql.Timestamp
import org.apache.spark.sql.Row

import java.text.SimpleDateFormat
import java.util.Date;
import java.util.Calendar;

object opptyLastQuoteTimeCalculator {
  /*def findDiff(a: String, b: String): Long = {
    val inputFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    if (a == null || b == null) {
      return 0
    }
    val sdate = inputFormat.parse(a);
    val edate = inputFormat.parse(b);
    sdate.getTime()
    return (sdate.getTime() - edate.getTime())
  }
  def main(args: Array[String]): Unit = {
    val testfile = "C:/Users/nookabh/Desktop/json/oppdetail.csv";
    val jobName = "OptyHistory"
    val conf = new SparkConf().setMaster("local[2]").setAppName(jobName)
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val sparkSession = SparkSession.builder
      .appName("spark session example").master("local[2]").appName(jobName)
      .config("spark.sql.warehouse.dir", "file:///C:/Users/nookabh/Desktop/json")
      .getOrCreate()

    val spark = SparkSession.builder.master("local[4]").appName("Oppertunity quote Analysis")
      .getOrCreate()

    case class OppertunityQuoteBody(oppurtunityId: String, oppCreatedDate: String, quoteId: String, quoteCreatedDate: String)
    import spark.implicits._
    val oppertunities_row_df = sparkSession.read.option("header", "true").csv(testfile)
    //Encoder for Oppertunity Body
    implicit val oppertunityEncoder = org.apache.spark.sql.Encoders.kryo[OppertunityQuoteBody] //serializer

    //1. Map by everything to Oppertunity Body Object
    val oppertunityObj_ds: Dataset[OppertunityQuoteBody] =
      oppertunities_row_df.
        map(row => OppertunityQuoteBody(row.apply(0) + "", row.apply(1) + "", row.apply(2) + "", row.apply(3) + ""))

    //2. Map by oppId and group them by Oppertunity Id 
    val oppertunityIdGroups = oppertunityObj_ds.map(oppertunity => {
      (oppertunity.oppurtunityId, oppertunity.quoteCreatedDate, oppertunity.oppCreatedDate)
    }).groupByKey(tuple => tuple._1)

    oppertunityIdGroups.count().show()
    
    //3. get the timestamp of the latestQuoteCreatedTime,Get the oppcreatedtime of it & get the timedifference from quotecreatedTime and oppcreatedtime
    //get the time difference between the lastquote and oppcreatedtime

    val reducedgroup = oppertunityIdGroups.reduceGroups(
      (elem1, elem2) => {
        val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        val ctime = findDiff(elem1._2, elem1._3)
        val time = formatter.format(ctime)

        val inputFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        val quotedate1 = inputFormat.parse(elem1._2);
        val quotedate2 = inputFormat.parse(elem2._2);

        if (quotedate1.getTime > quotedate2.getTime) {
          elem1
        } else
          elem2
      }).foreach { x =>
        println(x)
        findDiff(x._2._2, x._2._3)
        println("oppId=" + x._2._1 + "-" + findDiff(x._2._2, x._2._3))
      }

  }*/
}