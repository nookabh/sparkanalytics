package com.examples
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
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.hive.HiveContext
object TestDataforBusiness {
	def main(arg: Array[String]) {
	  
	val isServer = false;
		
	  if (isServer == true) {
			System.setProperty("hadoop.home.dir", "/opt/mapr/hadoop/hadoop-2.5.1/etc/hadoop"); //For server run 
		} else {
			System.setProperty("hadoop.home.dir", "C:\\winutils");  //For local run    
		}
	  
	  	  val jobName = "TestData"
	  	  var naspid = "10BBWI"
				var conf = new SparkConf().setMaster("local[2]").setAppName(jobName)
				var csvInPath_quote = "C:/SparkAnalytics/in/CX_PQ_089012017.txt"
				var csvInPath_opp = "C:/SparkAnalytics/in/cx_opptyhist_oppty_sfdc.txt"
				var csvInPath_filter = "C:/SparkAnalytics/in/Opp with filter date.csv"
				var csvInPath_gsat = "C:/SparkAnalytics/in/cx_GSAT_Master All.csv"
				var csvInPath_gct = "C:/SparkAnalytics/in/GCT data All.csv"
				var outFileName_quote = DateTimeFormat.forPattern("YYYY_MM_dd").print(System.currentTimeMillis) + "_QUOTE_TEST"
				var outFileName_opp = DateTimeFormat.forPattern("YYYY_MM_dd").print(System.currentTimeMillis) + "_OPP_TEST"
				var outFileName_oppfilter = DateTimeFormat.forPattern("YYYY_MM_dd").print(System.currentTimeMillis) + "_OPPfilter_TEST"
				var outFileName_gsat = DateTimeFormat.forPattern("YYYY_MM_dd").print(System.currentTimeMillis) + "_GSAT_TEST"
				var outFileName_gct = DateTimeFormat.forPattern("YYYY_MM_dd").print(System.currentTimeMillis) + "_GCT_TEST"
				var outputfileLoc_quote = "C:/SparkAnalytics/out/" + outFileName_quote
				var outputfileLoc_opp = "C:/SparkAnalytics/out/" + outFileName_opp
				var outputfileLoc_oppfilter = "C:/SparkAnalytics/out/" + outFileName_oppfilter
				var outputfileLoc_gsat = "C:/SparkAnalytics/out/" + outFileName_gsat
				var outputfileLoc_gct = "C:/SparkAnalytics/out/" + outFileName_gct
				
				if (isServer == true) {
					    outputfileLoc_quote = "/mapr/my.cluster.com/workflow/" + outFileName_quote
							outputfileLoc_opp = "/mapr/my.cluster.com/workflow/" + outFileName_opp
							csvInPath_quote = "/mapr/my.cluster.com/workflow/in/CX_PQ_089012017.csv"
							csvInPath_opp = "/mapr/my.cluster.com/workflow/cx_opptyhist_oppty_sfdc.txt"
							conf = new SparkConf().setAppName(jobName)
				} else {
					    conf = new SparkConf().setMaster("local[2]").setAppName(jobName)
					     Logger.getLogger("org").setLevel(Level.OFF)
               Logger.getLogger("akka").setLevel(Level.OFF)
				}					
	
	  	      
	      val sc = new SparkContext(conf)
				//val sqlContext = new org.apache.spark.sql.SQLContext(sc)
	      val sqlContext: SQLContext = new HiveContext(sc)
	      
	    def getHistoryDF(): DataFrame = {
      var csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/view_cx_opptyhist_oppty_sfdc.txt"
      var rawTextRDD = sc.textFile(csvInPath)
      rawTextRDD = rawTextRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      val quoteHistorySchemaFields = "OPPTY_Hist_ID|Oppty_id|CREATEDDATE|STAGENAME|SYSTEMMODSTAMP|SFDC_OPPORTUNITY_ID__C".split("\\|")
        .map(fieldName => StructField(fieldName, StringType, nullable = true))
      val quoteHistorySchema = StructType(quoteHistorySchemaFields)
      val rowRDD = rawTextRDD.map(_.split("\\|")).map(p => {
        if (p.length == 6) {
          val row: Row = Row(p(0), p(1), p(2), p(3), p(4), p(5))
          row
        } else {
          Row("", "", "", "", "", "")
        }
      })
      // Apply the schema to the RDD.
      val history_df = sqlContext.createDataFrame(rowRDD, quoteHistorySchema)
      history_df
    }

    def getBaseDF(): DataFrame = {
      var csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/CX_PQ_Milestone_PP.csv"
      var rawTextRDD = sc.textFile(csvInPath)
      rawTextRDD = rawTextRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      val quoteSchemaFields = "Quote_id|Quote_version|Quote_created_date|PP_Quote_Type|linked_quote_create_date|base_quote_id|Oppty_create_Date|Long_oppty_id|short_oppty_id|OPPORTUNITY_NAME|ACCOUNT_NAME|milestone_date|MILESTONE_ID|MILESTONE_DESC".split("\\|")
        .map(fieldName => StructField(fieldName, StringType, nullable = true))
      val quoteSchema = StructType(quoteSchemaFields)
      val rowRDD = rawTextRDD.map(_.split("\\|")).map(p => {
        if (p.length == 14) {
          val row: Row = Row(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13))
          row
        } else {
          Row("", "", "", "", "", "", "", "", "", "", "", "", "", "")
        }
      })
      // Apply the schema to the RDD.
      val base_df = sqlContext.createDataFrame(rowRDD, quoteSchema)
      base_df.show()
      base_df
    }
	   

	   

	    
	   def getGSATdata(): DataFrame = {
      var csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/view_cx_GSAT_Master.csv"
      var rawTextRDD = sc.textFile(csvInPath)
      rawTextRDD = rawTextRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      val quoteGSATSchemaFields = "ID,CREATEDDATE,SLO_COMMITMENT_DATE__C,COMPLETED_DATE__C,OPPORTUNITY_ID_SHORT__C".split(",")
        .map(fieldName => StructField(fieldName, StringType, nullable = true))
      val quoteGSATSchema = StructType(quoteGSATSchemaFields)
      val rowRDD = rawTextRDD.map(_.split(",")).map(p => {
        if (p.length == 5) {
          val row: Row = Row(p(0), p(1), p(2), p(3), p(4))
          row
        } else {
          Row("", "", "", "", "")
        }
      })
      // Apply the schema to the RDD.
      val quoteGSAT = sqlContext.createDataFrame(rowRDD, quoteGSATSchema)
      quoteGSAT.show()
      quoteGSAT
    }


	    def getGCTDF(): DataFrame = {
      var csvInPath = "C:/SparkAnalytics/in/GCT data All.csv"
      var rawTextRDD = sc.textFile(csvInPath)
      rawTextRDD = rawTextRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      val quoteHistorySchemaFields = "QUOTE_ID|QUOTE_VERSION_ID|DOCID|EVENT|MODIFICATION_DATE|MODIFICATION".split("\\|")
        .map(fieldName => StructField(fieldName, StringType, nullable = true))
      val quoteHistorySchema = StructType(quoteHistorySchemaFields)
      val rowRDD = rawTextRDD.map(_.split("\\,")).map(p => {
        if (p.length == 6) {
          val row: Row = Row(p(0), p(1), p(2), p(3), p(4), p(5))
          row
        } else {
          Row("", "", "", "", "", "")
        }
      })
      // Apply the schema to the RDD.
      val history_df = sqlContext.createDataFrame(rowRDD, quoteHistorySchema)
      history_df.show()
      history_df
    }
	    


	   
	   
	    	    
	      //**************************************************************
	      //* Start Test data for month of june                          *
	      //**************************************************************
	      
	      /*
				var base_df_quote = sqlContext.read.format("com.databricks.spark.csv")
				.option("header", "true") // Use first line of all files as header
				.option("inferSchema", "true") // Automatically infer data types
				.load(csvInPath_quote)
				
				def udfToTime_quote = udf((timeStamp: String) => { 
					val inputFormat = new SimpleDateFormat("M/d/yyyy' 'HH:mm:ss")  
					//val inputFormat = new SimpleDateFormat("M/d/yyyy")
					    //if(timeStamp != null){
							var date = inputFormat.parse(timeStamp) 
							date.getTime() 					     
					    //}

				})
				
				base_df_quote = base_df_quote.filter("milestone_date != ''")
				base_df_quote = base_df_quote.withColumn("milestone_sec", udfToTime_quote(base_df_quote("milestone_date")))
				
				var end_date = 	"07/01/2017"
	  	  val date_format = new SimpleDateFormat("MM/dd/yyyy")
	      var end_date_sec = date_format.parse(end_date).getTime()
	      var start_date = 	"06/01/2017"
	      var start_date_sec = date_format.parse(start_date).getTime()
	      
	      base_df_quote = base_df_quote.filter(base_df_quote.col("milestone_sec") <= end_date_sec)
	                      .filter(base_df_quote.col("milestone_sec") >= start_date_sec)  
	                      .drop(base_df_quote.col("milestone_sec"))
	      
	      
				base_df_quote.write.format("com.databricks.spark.csv")
				.mode("overwrite")
				.option("header", "true")
				.option("treatEmptyValuesAsNulls", "true") // No effect.
				.save(outputfileLoc_quote)		      
	      
	       var base_df_opp = sqlContext.read.format("com.databricks.spark.csv")
				.option("header", "true") // Use first line of all files as header
				.option("inferSchema", "true") // Automatically infer data types
				.option("delimiter", "|")
				.load(csvInPath_opp)
				
				
				def udfToTime = udf((timeStamp: String) => {  
					val inputFormat = new SimpleDateFormat("MM/dd/yyyy' 'HH:mm:ss")
							var date = inputFormat.parse(timeStamp) 
							date.getTime() 
				})
				
				
				base_df_opp = base_df_opp.withColumn("timestampsec", udfToTime(base_df_opp("CREATEDDATE")))
				var all_status_df = base_df_opp.select ("STAGENAME")
				all_status_df = all_status_df.dropDuplicates()
				println("display")
				all_status_df.show(20)
		    base_df_opp = base_df_opp.filter(base_df_opp.col("timestampsec") <= end_date_sec)
		                    .filter(base_df_opp.col("timestampsec") >= start_date_sec)
	                      .drop(base_df_opp.col("timestampsec"))
	                      
				base_df_opp.write.format("com.databricks.spark.csv")
				.mode("overwrite")
				.option("header", "true")
				.option("treatEmptyValuesAsNulls", "true") // No effect.
				.save(outputfileLoc_opp)
				
				*/
	      //**************************************************************
	      //* End of Test data for month of june                         *
	      //**************************************************************				
	        
				
	      //**************************************************************
	      //* Start Test data for opps closed in month of june           *
	      //**************************************************************
				
	       /*  
	   
        /*var base_df_opp = sqlContext.read.format("com.databricks.spark.csv")
				.option("header", "true") // Use first line of all files as header
				.option("inferSchema", "true") // Automatically infer data types
				.option("delimiter", "|")
				.load(csvInPath_opp)*/
	      var base_df_quote = getBaseDF()
				var base_df_opp = getHistoryDF()
				var base_df_gct = getGCTDF()
				
				def timeToStr = udf((milliSec: Long) => { 
			    DateTimeFormat.forPattern("MM/dd/YYYY").print(milliSec)  
		  })
		  

				def udfToTime = udf((timeStamp: String) => {  
					val inputFormat = new SimpleDateFormat("MM/dd/yyyy' 'HH:mm:ss")
					var date = inputFormat.parse(timeStamp) 
					date.getTime() 
				})
				
				base_df_opp = base_df_opp.withColumn("timestampsec", udfToTime(base_df_opp("CREATEDDATE")))
				                         .withColumn("Prev_Stage", lag("STAGENAME", 1).over(Window.partitionBy("SFDC_OPPORTUNITY_ID__C").orderBy("timestampsec")))
      base_df_opp.show()
      base_df_opp = base_df_opp.na.fill("")
      base_df_opp = base_df_opp.filter("STAGENAME != Prev_Stage")
				var OppMax_df = base_df_opp.groupBy("SFDC_OPPORTUNITY_ID__C").agg(max("timestampsec"))
				OppMax_df = OppMax_df.withColumnRenamed("SFDC_OPPORTUNITY_ID__C" , "SFDC_OPPORTUNITY_ID")
				
				var OppStage_df = base_df_opp.join(
				    OppMax_df, 
				    base_df_opp("SFDC_OPPORTUNITY_ID__C") === OppMax_df("SFDC_OPPORTUNITY_ID")
		      && base_df_opp("timestampsec") === OppMax_df("max(timestampsec)"))				
				
		      
				var end_date = 	"07/01/2017"
	  	  val date_format = new SimpleDateFormat("MM/dd/yyyy")
	      var end_date_sec = date_format.parse(end_date).getTime()
	      var start_date = 	"06/01/2017"
	      var start_date_sec = date_format.parse(start_date).getTime()
	      
		    OppStage_df = OppStage_df.filter(OppStage_df.col("timestampsec") <= end_date_sec)
		                    .filter(OppStage_df.col("timestampsec") >= start_date_sec)
	                      .drop(OppStage_df.col("timestampsec"))
	                      .filter("STAGENAME = '5 Closed Lost' or STAGENAME = '5 Closed Won' or STAGENAME = '5 Closed Disqualified'")
	                      
	      OppStage_df = OppStage_df.select("SFDC_OPPORTUNITY_ID__C")                
	      OppStage_df.count()                
	      base_df_opp = base_df_opp.join(OppStage_df, "SFDC_OPPORTUNITY_ID__C")
	      base_df_opp.show()
	      base_df_opp.drop("timestampsec")
				base_df_opp.write.format("com.databricks.spark.csv")
				.mode("overwrite")
				.option("header", "true")
				.option("treatEmptyValuesAsNulls", "true") // No effect.
				.save(outputfileLoc_opp)	      
	      
				/*var base_df_quote = sqlContext.read.format("com.databricks.spark.csv")
				.option("header", "true") // Use first line of all files as header
				.option("inferSchema", "true") // Automatically infer data types
				.load(csvInPath_quote)*/
				
				base_df_quote = base_df_quote.join(OppStage_df,
				    base_df_quote("short_oppty_id") === OppStage_df("SFDC_OPPORTUNITY_ID__C")
				    )
				base_df_quote.write.format("com.databricks.spark.csv")
				.mode("overwrite")
				.option("header", "true")
				.option("treatEmptyValuesAsNulls", "true") // No effect.
				.save(outputfileLoc_quote)
				
				/*var base_df_gsat = sqlContext.read.format("com.databricks.spark.csv")
				.option("header", "true") // Use first line of all files as header
				.option("inferSchema", "true") // Automatically infer data types
				.load(csvInPath_gsat)*/
				var base_df_gsat = getGSATdata()
				base_df_gsat = base_df_gsat.join(OppStage_df,
				    base_df_gsat("OPPORTUNITY_ID_SHORT__C") === OppStage_df("SFDC_OPPORTUNITY_ID__C")
				    )
				    
				base_df_gsat = base_df_gsat.drop("OPPORTUNITY_ID_SHORT__C")

				base_df_gsat.write.format("com.databricks.spark.csv")
				.mode("overwrite")
				.option("header", "true")
				.option("treatEmptyValuesAsNulls", "true") // No effect.
				.save(outputfileLoc_gsat)
				
				var valid_quotes_df = base_df_quote.select("Quote_id")
				valid_quotes_df = valid_quotes_df.dropDuplicates()
				
				/*var base_df_gct = sqlContext.read.format("com.databricks.spark.csv")
				.option("header", "true") // Use first line of all files as header
				.option("inferSchema", "true") // Automatically infer data types
				.load(csvInPath_gct)*/
				
				base_df_gct.printSchema()
				valid_quotes_df.printSchema()
				base_df_gct = base_df_gct.join(valid_quotes_df,
				    base_df_gct("QUOTE_ID") === valid_quotes_df("Quote_id")
				    )				
				base_df_gct = base_df_gct.drop("Quote_id")

				base_df_gct.write.format("com.databricks.spark.csv")
				.mode("overwrite")
				.option("header", "true")
				.option("treatEmptyValuesAsNulls", "true") // No effect.
				.save(outputfileLoc_gct)				
				    
				*/
	    
				//**************************************************************
	      //* End Test data for opps closed in month of june           *
	      //**************************************************************				
				
				
			
	      //**************************************************************
	      //* Start Test data for opps for a NASP                        *
	      //**************************************************************
				
	      /*
        var base_df_opp = sqlContext.read.format("com.databricks.spark.csv")
				.option("header", "true") // Use first line of all files as header
				.option("inferSchema", "true") // Automatically infer data types
				.option("delimiter", "|")
				.load(csvInPath_opp)
				
				def timeToStr = udf((milliSec: Long) => { 
			    DateTimeFormat.forPattern("MM/dd/YYYY").print(milliSec)  
		  })
		  

				def udfToTime = udf((timeStamp: String) => {  
					val inputFormat = new SimpleDateFormat("MM/dd/yyyy' 'HH:mm:ss")
					var date = inputFormat.parse(timeStamp) 
					date.getTime() 
				})
				
				 var base_df_nasp = sqlContext.read.format("com.databricks.spark.csv")
				.option("header", "true") // Use first line of all files as header
				.option("inferSchema", "true") // Automatically infer data types
				.option("delimiter", "|")
				.load(csvInPath_nasp)
        
				var OppNasp_df =  base_df_nasp.filter(base_df_nasp.col("ACCOUNT_NASP_NAME__C") === naspid)
	      OppNasp_df = OppNasp_df.select("SFDC_OPPORTUNITY_ID__C")
	      OppNasp_df = OppNasp_df.dropDuplicates() 
	      OppNasp_df.count()                
	      base_df_opp = base_df_opp.join(OppNasp_df, "SFDC_OPPORTUNITY_ID__C")
	      base_df_opp.show()
	      
				base_df_opp.write.format("com.databricks.spark.csv")
				.mode("overwrite")
				.option("header", "true")
				.option("treatEmptyValuesAsNulls", "true") // No effect.
				.save(outputfileLoc_opp)	      
	      
				var base_df_quote = sqlContext.read.format("com.databricks.spark.csv")
				.option("header", "true") // Use first line of all files as header
				.option("inferSchema", "true") // Automatically infer data types
				.load(csvInPath_quote)
				
				base_df_quote = base_df_quote.join(OppNasp_df,
				    base_df_quote("short_oppty_id") === OppNasp_df("SFDC_OPPORTUNITY_ID__C")
				    )
				base_df_quote.write.format("com.databricks.spark.csv")
				.mode("overwrite")
				.option("header", "true")
				.option("treatEmptyValuesAsNulls", "true") // No effect.
				.save(outputfileLoc_quote)
				
				var base_df_gsat = sqlContext.read.format("com.databricks.spark.csv")
				.option("header", "true") // Use first line of all files as header
				.option("inferSchema", "true") // Automatically infer data types
				.load(csvInPath_gsat)
				
				base_df_gsat = base_df_gsat.join(OppNasp_df,
				    base_df_gsat("OPPORTUNITY_ID_SHORT__C") === OppNasp_df("SFDC_OPPORTUNITY_ID__C")
				    )
				    
				base_df_gsat = base_df_gsat.drop("OPPORTUNITY_ID_SHORT__C")

				base_df_gsat.write.format("com.databricks.spark.csv")
				.mode("overwrite")
				.option("header", "true")
				.option("treatEmptyValuesAsNulls", "true") // No effect.
				.save(outputfileLoc_gsat)
				
				var valid_quotes_df = base_df_quote.select("Quote_id")
				valid_quotes_df = valid_quotes_df.dropDuplicates()
				
				var base_df_gct = sqlContext.read.format("com.databricks.spark.csv")
				.option("header", "true") // Use first line of all files as header
				.option("inferSchema", "true") // Automatically infer data types
				.load(csvInPath_gct)
				base_df_gct.printSchema()
				valid_quotes_df.printSchema()
				base_df_gct = base_df_gct.join(valid_quotes_df,
				    base_df_gct("QUOTE_ID") === valid_quotes_df("Quote_id")
				    )				
				base_df_gct = base_df_gct.drop("Quote_id")

				base_df_gct.write.format("com.databricks.spark.csv")
				.mode("overwrite")
				.option("header", "true")
				.option("treatEmptyValuesAsNulls", "true") // No effect.
				.save(outputfileLoc_gct)				
				    
				*/
				//**************************************************************
	      //* End Test data for opps in a NASP                           *
	      //**************************************************************				
				
				//**************************************************************
				// start of temp filter data code                              *
				//**************************************************************

								
	   def getOppFilter(): DataFrame = {
      var csvInPath = "C:/SparkAnalytics/in/Opp filter 10-23.csv"
      var rawTextRDD = sc.textFile(csvInPath)
      rawTextRDD = rawTextRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      val quoteSchemaFields = "Account_Name__c|Bookings__c|Closed_Date__c|Director__c|GCH_Id__c|Geography__c|GVP__c|Is_VRD__c|Manager__c|NASP_Id__c|NASP_Name__c|Opp_Name__c|Opp_Stage__c|PR1__c|PR2__c|PR3__c|PR4__c|Products_Count__c|Segment__c|SFDC_Acc_Id__c|SVP__c|SFDC_Opp_Id__c".split("\\|")
        .map(fieldName => StructField(fieldName, StringType, nullable = true))
      val quoteSchema = StructType(quoteSchemaFields)
      val rowRDD = rawTextRDD.map(_.split("\\|")).map(p => {
        var pCleans = ListBuffer[String]()
       for (a <- p){
         pCleans += a.replaceAll("'", "").replaceAll("\"","")
       }
      var pCleansList:List[String] = pCleans.toList;
        if (p.length == 22) {
          val row: Row = Row(pCleansList(0), pCleansList(1), pCleansList(2), pCleansList(3), pCleansList(4), pCleansList(5), pCleansList(6), pCleansList(7), pCleansList(8), pCleansList(9), pCleansList(10), pCleansList(11), pCleansList(12),pCleansList(13), pCleansList(14), pCleansList(15), pCleansList(16), pCleansList(17), pCleansList(18), pCleansList(19), pCleansList(20), pCleansList(21))
          row
        } else {
          Row("", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "")
        }
      })
      // Apply the schema to the RDD.
      val base_df = sqlContext.createDataFrame(rowRDD, quoteSchema)
      base_df.show(100)
      base_df
    }
    
    	   def getTestdata(): DataFrame = {
      var csvInPath = "C:/SparkAnalytics/Test_data_for_Business/Opp Closed in June with filter 10-20.csv"
      var rawTextRDD = sc.textFile(csvInPath)
      rawTextRDD = rawTextRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      val quoteSchemaFields = "SFDC_OPPORTUNITY_ID__C,OPPTY_Hist_ID,Oppty_id,CREATEDDATE,Prev_Stage,STAGENAME".split("\\,")
        .map(fieldName => StructField(fieldName, StringType, nullable = true))
      val quoteSchema = StructType(quoteSchemaFields)
      val rowRDD = rawTextRDD.map(_.split("\\,")).map(p => {
                var pCleans = ListBuffer[String]()
       for (a <- p){
         pCleans += a.replaceAll("'", "").replaceAll("\"","")
       }
      var pCleansList:List[String] = pCleans.toList;
      if (p.length == 6) {
          val row: Row = Row(pCleansList(0), pCleansList(1), pCleansList(2), pCleansList(3), pCleansList(4), pCleansList(5))
          row
        } else {
          Row("", "", "", "", "", "")
        }
      })
      // Apply the schema to the RDD.
      val history_df = sqlContext.createDataFrame(rowRDD, quoteSchema)
      history_df.show(100)
      history_df
    }
    
    
    				 
				var juneclosedopp_df = getTestdata()
				var filter_df = getOppFilter()
				juneclosedopp_df = juneclosedopp_df.join(filter_df, juneclosedopp_df("SFDC_OPPORTUNITY_ID__C") === filter_df("SFDC_Opp_Id__c"), "left_outer")
				juneclosedopp_df.write.format("com.databricks.spark.csv")
				.mode("overwrite")
				.option("header", "true")
				.option("treatEmptyValuesAsNulls", "true") // No effect.
				.save(outputfileLoc_oppfilter)
				  
				
				//**************************************************************
				// end of temp filter data code                                *
				//**************************************************************				
	}
  
}