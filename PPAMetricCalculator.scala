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
	

object PPAMetricCalculator {

	def main(arg: Array[String]) {
	  
  //Logger.getLogger("org").setLevel(Level.OFF)
  //Logger.getLogger("akka").setLevel(Level.OFF)
	  val isServer = true;
		
	  if (isServer == true) {
			System.setProperty("hadoop.home.dir", "/opt/mapr/hadoop/hadoop-2.5.1/etc/hadoop"); //For server run 
		} else {
			System.setProperty("hadoop.home.dir", "C:\\winutils");  //For local run    
		}
	  
	      val jobName = "PPA"
				var conf = new SparkConf().setMaster("local[2]").setAppName(jobName)
				var csvInPath_quote = "C:/SparkAnalytics/CX_milestone_siva.csv"
				var csvInPath_opp = "C:/SparkAnalytics/Opp_history_pipe.txt"
				var outFileName_PPACount = DateTimeFormat.forPattern("YYYY_MM_dd").print(System.currentTimeMillis) + "_PPA_COUNT"
				var outFileName_PPAPercent = DateTimeFormat.forPattern("YYYY_MM_dd").print(System.currentTimeMillis) + "_PPA_PERCENT"
				var outputfileLoc_PPACount = "C:/SparkAnalytics/" + outFileName_PPACount
				var outputfileLoc_PPAPercent = "C:/SparkAnalytics/" + outFileName_PPAPercent
				
				if (isServer == true) {
					    outputfileLoc_PPACount = "/mapr/my.cluster.com/workflow/" + outFileName_PPACount
							outputfileLoc_PPAPercent = "/mapr/my.cluster.com/workflow/" + outFileName_PPAPercent
							csvInPath_quote = "/mapr/my.cluster.com/workflow/in/CX_PQ_089012017.csv"
							csvInPath_opp = "/mapr/my.cluster.com/workflow/cx_opptyhist_oppty_sfdc.txt"
							conf = new SparkConf().setAppName(jobName)
				} else {
					    conf = new SparkConf().setMaster("local[2]").setAppName(jobName)
					     Logger.getLogger("org").setLevel(Level.OFF)
               Logger.getLogger("akka").setLevel(Level.OFF)
				}				
	  
	      val sc = new SparkContext(conf)
				val sqlContext = new org.apache.spark.sql.SQLContext(sc)		    
				var base_df = sqlContext.read.format("com.databricks.spark.csv")
				.option("header", "true") // Use first line of all files as header
				.option("inferSchema", "true") // Automatically infer data types
//				.option("delimiter", "|")
				.load(csvInPath_quote)
				
				base_df.show(10)
				base_df.printSchema()
				
				var base_quote_df = base_df.filter("MILESTONE_ID = '10001010'")
				base_quote_df = base_df.filter("PP_Quote_Type = 'Base Quote'")
				
				base_quote_df = base_quote_df.drop(base_quote_df.col("Quote_id"))
				base_quote_df = base_quote_df.drop(base_quote_df.col("Quote_version"))
				base_quote_df = base_quote_df.drop(base_quote_df.col("PP_Quote_Type"))
				base_quote_df = base_quote_df.drop(base_quote_df.col("linked_quote_create_date"))
				base_quote_df = base_quote_df.drop(base_quote_df.col("base_quote_id"))
				base_quote_df = base_quote_df.drop(base_quote_df.col("Oppty_create_Date"))
				base_quote_df = base_quote_df.drop(base_quote_df.col("Long_oppty_id"))
				//base_quote_df = base_quote_df.drop(base_quote_df.col("OPPORTUNITY_NAME"))
				//base_quote_df = base_quote_df.drop(base_quote_df.col("ACCOUNT_NAME"))
				//base_quote_df = base_quote_df.drop(base_quote_df.col("milestone_date"))
				base_quote_df = base_quote_df.drop(base_quote_df.col("MILESTONE_ID"))
				//base_quote_df = base_quote_df.drop(base_quote_df.col("MILESTONE_DESC"))
				
				base_quote_df = base_quote_df.dropDuplicates()
				base_quote_df.show()
				base_quote_df = base_quote_df.filter(base_quote_df.col("milestone_date").isNotNull)
				base_quote_df = base_quote_df.filter("milestone_date != ''")
				println("not null")
				base_quote_df.show()
				
				def udfToTime_quote = udf((timeStamp: String) => { 
					val inputFormat = new SimpleDateFormat("M/d/yyyy' 'HH:mm:ss")  
					//val inputFormat = new SimpleDateFormat("M/d/yyyy")
					    //if(timeStamp != null){
							var date = inputFormat.parse(timeStamp) 
							date.getTime() 					     
					    //}

				})
				
				base_quote_df = base_quote_df.withColumn("milestone_sec", udfToTime_quote(base_quote_df("milestone_date")))
				base_quote_df.show()
				base_quote_df = base_quote_df.groupBy("short_oppty_id").agg(min("milestone_sec"))
	  	  
  			var base_df_opp = sqlContext.read.format("com.databricks.spark.csv")
				.option("header", "true") // Use first line of all files as header
				.option("inferSchema", "true") // Automatically infer data types
				.option("delimiter", "|")
				.load(csvInPath_opp)
				
				base_df_opp.show()
		  
			  def timeToStr = udf((milliSec: Long) => { 
			    DateTimeFormat.forPattern("MM/dd/YYYY").print(milliSec)  
		    })
		    
				def udfToTime = udf((timeStamp: String) => {  
					val inputFormat = new SimpleDateFormat("MM/dd/yyyy' 'HH:mm:ss")
							var date = inputFormat.parse(timeStamp) 
							date.getTime() 
				})
				
				val schema = StructType(
        StructField("SFDCOppId", StringType, true) ::
        StructField("EventName", StringType, true) :: 
        StructField("EventType", StringType, true) ::
        StructField("EventStartDate", StringType, true) ::
        StructField("EventCompletionDate", StringType, true) ::
        StructField("Count", StringType, true) ::
        Nil) 
        var PPAcountall_df =  sqlContext.createDataFrame(sc.emptyRDD[Row], schema)
        var PPA_out_df = sqlContext.createDataFrame(sc.emptyRDD[Row], schema)
    
	  	  var run_date = 	"04/16/2017"
	  	  val date_format = new SimpleDateFormat("MM/dd/yyyy")
	      var run_date_sec = date_format.parse(run_date).getTime()
	      println(run_date_sec)
	      var  cal_rundate = Calendar.getInstance();
	  	  var  cal_today = Calendar.getInstance();    
        cal_rundate.setTime( date_format.parse(run_date)); 
	      
        println(cal_rundate.getTimeInMillis())
        println(cal_today.getTimeInMillis())
        
        
				base_df_opp = base_df_opp.drop(base_df_opp.col("OPPTY_Hist_ID"))
				base_df_opp = base_df_opp.drop(base_df_opp.col("SYSTEMMODSTAMP"))	
				
				base_df_opp = base_df_opp.withColumn("timestampsec", udfToTime(base_df_opp("CreatedDate")))
				//var opp_hist_period_df = null
				//for (i <- 1 to 1){
				while(cal_rundate.compareTo(cal_today) < 0 ) {  
				  
				  run_date_sec = cal_rundate.getTimeInMillis()
				  if (isServer == true) {
				    outFileName_PPACount = DateTimeFormat.forPattern("YYYY_MM_dd").print(run_date_sec) + "_PPA_COUNT"
				    outputfileLoc_PPACount = "/mapr/my.cluster.com/workflow/" + outFileName_PPACount
				  }else {
				    outFileName_PPACount = DateTimeFormat.forPattern("YYYY_MM_dd").print(run_date_sec) + "_PPA_COUNT"
				    outputfileLoc_PPACount = "C:/SparkAnalytics/" + outFileName_PPACount
				  } 
				  
				   println(run_date_sec)
				var opp_hist_period_df = base_df_opp.filter(base_df_opp.col("timestampsec") <= run_date_sec)
				opp_hist_period_df.show()
				
				
		  
			var max_stage_df = opp_hist_period_df.groupBy("SFDC_OPPORTUNITY_ID__C").agg(max("timestampsec"))
			max_stage_df = max_stage_df.withColumnRenamed("SFDC_OPPORTUNITY_ID__C" , "SFDC_OPPORTUNITY_ID")		  
			var current_stage_df = opp_hist_period_df.join(
		      max_stage_df,
		      opp_hist_period_df("SFDC_OPPORTUNITY_ID__C") === max_stage_df("SFDC_OPPORTUNITY_ID")
		      && opp_hist_period_df("timestampsec") === max_stage_df("MAX(timestampsec)")
		      )
		      
		  var base_quote_period_df = base_quote_df.join(current_stage_df, base_quote_df("short_oppty_id") === current_stage_df("SFDC_OPPORTUNITY_ID__C"))
		      base_quote_period_df = base_quote_period_df.filter(base_quote_period_df.col("MIN(milestone_sec)") <= run_date_sec )  		  
		  
		      
		      //current_stage_df.drop("SFDC_OPPORTUNITY_ID__C")

			current_stage_df = current_stage_df.filter("StageName != '5 Closed Lost'")
			current_stage_df = current_stage_df.filter("StageName != '5 Closed Disqualified'")
			current_stage_df = current_stage_df.filter("StageName != '0 Identify'")
			current_stage_df = current_stage_df.filter("StageName != '1 Qualify'")
			//current_stage_df = current_stage_df.filter("StageName != '5 Closed Lost'")
			println("base_quote_period_df")
			base_quote_period_df.show()
		  println("current_stage_df")
			current_stage_df.show()
		  
			base_quote_period_df = base_quote_period_df.withColumn("is_basequote", lit("1") ).select("short_oppty_id", "is_basequote")
			println("base_quote_period_df")
			base_quote_period_df.show()
		  var ppa_count = base_quote_period_df.count()
//		  var ppa_percent = base_quote_period_df.count()/current_stage_df.count() * 100
		  
		  println(ppa_count)
//	    println(ppa_percent)  
  		current_stage_df = current_stage_df.join(base_quote_period_df, current_stage_df("SFDC_OPPORTUNITY_ID__C") === base_quote_period_df("short_oppty_id"), "left_outer")
				current_stage_df = current_stage_df
				.withColumn("EventType", lit("TRENDING"))
				.withColumn("EventName", lit("PPA_COUNT"))
				.withColumn("EventStartDate", lit(""))
				.withColumn("EventCompletionDate", timeToStr(lit(run_date_sec)))
				.withColumn("Count", current_stage_df("is_basequote") )
				.withColumn("SFDCOppId", current_stage_df("SFDC_OPPORTUNITY_ID__C"))	 
				
				var PPAcount_df = current_stage_df.select("SFDCOppId", "EventName", "EventType", "EventStartDate", "EventCompletionDate", "Count")
				
				PPAcountall_df = PPAcountall_df.unionAll(PPAcount_df)
				PPAcount_df = PPAcount_df.withColumn("Count", when(PPAcount_df("Count").isNull, lit("0")).otherwise(PPAcount_df("Count")))
	      /*PPAcount_df.coalesce(1).write.format("com.databricks.spark.csv")
				.mode("overwrite")
				.option("header", "true")
				.option("treatEmptyValuesAsNulls", "true") // No effect.
				.save(outputfileLoc_PPACount)*/
				

				  cal_rundate.add( Calendar.DATE, 7 );
				  println(run_date_sec)

				  
				}
	      
				PPAcountall_df = PPAcountall_df.withColumn("Count", when(PPAcountall_df("Count").isNull, lit("0")).otherwise(PPAcountall_df("Count")))
	      PPAcountall_df.write.format("com.databricks.spark.csv")
				.mode("overwrite")
				.option("header", "true")
				.option("treatEmptyValuesAsNulls", "true") // No effect.
				.save(outputfileLoc_PPACount)
				
				
				
				
				/*
				var PPApercent_df = current_stage_df
				.withColumn("EventType", lit("TRENDING"))
				.withColumn("EventName", lit("PPA_PERCENT"))
				.withColumn("EventStartDate", lit(""))
				.withColumn("EventCompletionDate", timeToStr(current_stage_df("MAX(timestampsec)")))
				.withColumn("Count", lit(ppa_percent) )
				.withColumn("SFDCOppId", current_stage_df("SFDC_OPPORTUNITY_ID__C"))	 
				
				PPApercent_df.coalesce(1).write.format("com.databricks.spark.csv")
				.mode("overwrite")
				.option("header", "true")
				.option("treatEmptyValuesAsNulls", "true") // No effect.
				.save(outputfileLoc_PPAPercent)					
    		*/
				



}
}