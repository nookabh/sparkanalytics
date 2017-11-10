package com.examples
  /*
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
//import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.udf
import java.text.SimpleDateFormat
//import com.lucidworks.spark.rdd.SolrRDD

import com.google.gson.Gson

case class OpportunityStatus(
    createdTimestamp: String,
    id: String,
    OpportunityId:String,
    StageName:String,
    SystemModstamp:String
)

object WriteToSolr {
  def main(arg: Array[String]) {
    //System.setProperty("hadoop.home.dir", "C:\\winutils");
    var logger = Logger.getLogger(this.getClass())
    val jobName = "OptyHistory"
    val conf = new SparkConf().setMaster("local[2]").setAppName(jobName)
    val sc = new SparkContext(conf)
    val test = "";
    logger.info("Opty History analysis");
    logger.info("=> jobName \"" + jobName + "\"")
    //windows
     val sparkSession = SparkSession.builder
      .appName("spark session example").master("local[2]").appName(jobName)
      .config("spark.sql.warehouse.dir", "file:///c:/ac/TestData/scala")
      .getOrCreate()
    val path = "c:/ac/TestData/scala/test1.csv"
    // val path = "c:/ac/TestData/scala/Opp_History_Staging.csv"
    var base_df = sparkSession.read.option("header", "true").csv(path)
    var counter: Int = 0
    base_df.show(5)
    base_df.collect().map(t => {
      val obj: OpportunityStatus = OpportunityStatus(t.getAs("createdTimestamp"),t.getAs("id"), t.getAs("OpportunityId"),t.getAs("StageName"),t.getAs("SystemModstamp"))
      val gson = new Gson
      val jsonString = gson.toJson(obj)
      println(jsonString)
       
    })
      
    //write to solr
      val options = Map(
      "zkhost" -> "gchsc1lna062.itcent.ebiz.verizon.com:6988",
      "collection" -> "spark-solr-workflow",
      "gen_uniq_key" -> "true" // Generate unique key if the 'id' field does not exist
      )
      base_df.write.format("solr").options(options).mode(org.apache.spark.sql.SaveMode.Overwrite).save
    
    
  }
}*/