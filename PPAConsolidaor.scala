package com.workflow
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.udf
import java.text.SimpleDateFormat
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.types.{
    StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.Row
import org.apache.log4j.Level
import java.util.Calendar

object PPAConsolidaor {
  
  	def main(arg: Array[String]) {
  	  Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
  	  val jobName = "PPA_ALL"
  	  val isServer = true;
		
	  if (isServer == true) {
			System.setProperty("hadoop.home.dir", "/opt/mapr/hadoop/hadoop-2.5.1/etc/hadoop"); //For server run 
		} else {
			System.setProperty("hadoop.home.dir", "C:\\winutils");  //For local run    
		}  	
  	
	  var conf = new SparkConf().setMaster("local[2]").setAppName(jobName)
	  var outFileName_PPACount = DateTimeFormat.forPattern("YYYY_MM_dd").print(System.currentTimeMillis) + "_PPA_COUNT_ALL"

				if (isServer == true) {
				  conf = new SparkConf().setAppName(jobName)
				} else {
					    conf = new SparkConf().setMaster("local[2]").setAppName(jobName)
					    
				}
	  
	  	      val sc = new SparkContext(conf)
				val sqlContext = new org.apache.spark.sql.SQLContext(sc)	
	  	  var run_date = 	"2016_01_03"
	  	  val date_format = new SimpleDateFormat("yyyy_MM_dd")
	  	  var  cal_rundate = Calendar.getInstance();
	  	  var  cal_today = Calendar.getInstance();    
        cal_rundate.setTime( date_format.parse(run_date));    
         
	      var run_date_sec = date_format.parse(run_date).getTime()
	      println(date_format.format(cal_rundate.getTime()))
				
	      val schema = StructType(
        StructField("SFDCOppId", StringType, true) ::
        StructField("EventName", StringType, true) :: 
        StructField("EventType", StringType, true) ::
        StructField("EventStartDate", StringType, true) ::
        StructField("EventCompletionDate", StringType, true) ::
        StructField("Count", StringType, true) ::
        Nil) 
        var PPAcountall_df =  sqlContext.createDataFrame(sc.emptyRDD[Row], schema)	  	      
				
        println(cal_rundate.getTime())
        println(cal_today.getTime())
        println(cal_rundate.compareTo(cal_today))
        
        //for (i <- 1 to 89){
          while(cal_rundate.compareTo(cal_today) < 0 ) {
				  var csvInPath = "/mapr/my.cluster.com/workflow/" + date_format.format(cal_rundate.getTime()) + "_PPA_COUNT/part-00000"
				  println(csvInPath)
				  cal_rundate.add( Calendar.DATE, 7 );
			
				 var PPAcount_df = sqlContext.read.format("com.databricks.spark.csv")
				.option("header", "true") // Use first line of all files as header
				.option("inferSchema", "true") // Automatically infer data types
//				.option("delimiter", "|")
				.load(csvInPath)
				
				PPAcountall_df = PPAcountall_df.unionAll(PPAcount_df)
			  
				}
	      
	      PPAcountall_df.coalesce(1).write.format("com.databricks.spark.csv")
				.mode("overwrite")
				.option("header", "true")
				.option("treatEmptyValuesAsNulls", "true") // No effect.
				.save("/mapr/my.cluster.com/workflow/PPA_COUNT_METRIC_ALL")	      
	      	}
  
}