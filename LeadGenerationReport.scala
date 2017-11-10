package com.workflow.lead

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import java.text.SimpleDateFormat
import org.joda.time.format.DateTimeFormat
import scala.reflect.api.materializeTypeTag
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import scala.collection.mutable.ListBuffer



object LeadGenerationReport {

  val reportbase = new(MKReportBase)
  def convertDateTime(timeStamp: String):Long = {
			val inputFormat = new SimpleDateFormat("MM/dd/yyyy' 'HH:mm:ss")
			var date = inputFormat.parse(timeStamp) 
			date.getTime() 		
  }
  
  def getLeadHistoryDF(sqlContext:SQLContext,sc:SparkContext ): DataFrame = {
    var csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/LeadHistory2017_pipe.csv"
    var rawTextRDD = sc.textFile(csvInPath)
    rawTextRDD = rawTextRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    val leadSchemaFields = "Id|IsDeleted|LeadId|CreatedById|CreatedDate|Field|OldValue|NewValue".split("\\|")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val leadHistrySchema = StructType(leadSchemaFields)
    
    
    val rowRDD = rawTextRDD.map(_.split("\\|")).map(p => {
       var pCleans = ListBuffer[String]()
       for (a <- p){
         pCleans += a.replaceAll("'", "").replaceAll("\"","")
       }
      var pCleansList:List[String] = pCleans.toList;
      if (p.length == 8) {
        Row(pCleansList(0), pCleansList(1), pCleansList(2), pCleansList(3), pCleansList(4), pCleansList(5), pCleansList(6), pCleansList(7))
      } else {
        Row("", "", "", "", "", "", "", "")
      }
    })
    // Apply the schema to the RDD.
    val base_df = sqlContext.createDataFrame(rowRDD, leadHistrySchema)
    //base_df.show()
    base_df
  }
  
    def getActivityDF(sqlContext:SQLContext,sc:SparkContext ): DataFrame = {
    var csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/MarketoActivityReport_Since_May.csv"
    var rawTextRDD = sc.textFile(csvInPath)
    rawTextRDD = rawTextRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    val SchemaFields = "activityDate,campaignId,id,activityTypeId,leadId".split(",")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val leadHistrySchema = StructType(SchemaFields)
    val rowRDD = rawTextRDD.map(_.split(",")).map(p => {
       var pCleans = ListBuffer[String]()
       for (a <- p){
         pCleans += a.replaceAll("'", "").replaceAll("\"","")
       }
      var pCleansList:List[String] = pCleans.toList;
      if (p.length == 5) {
        val row: Row = Row(pCleansList(0), pCleansList(1), pCleansList(2), pCleansList(3), pCleansList(4))
        row
      } else {
        Row("", "", "", "", "")
      }
    })
    // Apply the schema to the RDD.
    val base_df = sqlContext.createDataFrame(rowRDD, leadHistrySchema)
    //base_df.show()
    base_df
  }
    

   def getLeadDF(sqlContext:SQLContext,sc:SparkContext ): DataFrame = {
    var csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/one_lead_09212017.csv"
    var rawTextRDD = sc.textFile(csvInPath)
    rawTextRDD = rawTextRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    val SchemaFields = "External_System_Lead_ID__c,External_Lead_Source_System__c,Status,CreatedDate,Id,SFDC_Lead_ID__c,OwnerId,Owner.Name,SFDC_Opportunity_ID__c".split(",")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val leadHistrySchema = StructType(SchemaFields)
    val rowRDD = rawTextRDD.map(_.split(",")).map(p => {
       var pCleans = ListBuffer[String]()
       for (a <- p){
         pCleans += a.replaceAll("'", "").replaceAll("\"","")
       }
      var pCleansList:List[String] = pCleans.toList;
      if (p.length == 9) {
        val row: Row = Row(pCleansList(0), pCleansList(1), pCleansList(2), pCleansList(3), pCleansList(4), pCleansList(5), pCleansList(6), pCleansList(7), pCleansList(8))
        row
      } else {
        Row("", "", "", "", "","", "", "","")
      }
    })
    // Apply the schema to the RDD.
    val base_df = sqlContext.createDataFrame(rowRDD, leadHistrySchema)
    //base_df.show()
    base_df
  }
  
     def getCampTypeDF(sqlContext:SQLContext,sc:SparkContext ): DataFrame = {
    var csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/Campaign_type_mapping_dummy.csv"
    var rawTextRDD = sc.textFile(csvInPath)
    rawTextRDD = rawTextRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    val SchemaFields = "campaignId,CampaignType".split(",")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val leadHistrySchema = StructType(SchemaFields)
    val rowRDD = rawTextRDD.map(_.split(",")).map(p => {
       var pCleans = ListBuffer[String]()
       for (a <- p){
         pCleans += a.replaceAll("'", "").replaceAll("\"","")
       }
      var pCleansList:List[String] = pCleans.toList;
      if (p.length == 2) {
        val row: Row = Row(pCleansList(0), pCleansList(1))
        row
      } else {
        Row("", "")
      }
    })
    // Apply the schema to the RDD.
    val base_df = sqlContext.createDataFrame(rowRDD, leadHistrySchema)
    base_df.show()
    base_df
  }
     
   def getCampDetailsDF(sqlContext:SQLContext,sc:SparkContext ): DataFrame = {
    var csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/CampaignDetails.csv"
    var rawTextRDD = sc.textFile(csvInPath)
    rawTextRDD = rawTextRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    val SchemaFields = "createdAt,active,workspaceName,id,type,programId,updatedAt,SFDCCampaignID,CampaignCost".split(",")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val leadHistrySchema = StructType(SchemaFields)
    val rowRDD = rawTextRDD.map(_.split(",")).map(p => {
       var pCleans = ListBuffer[String]()
       for (a <- p){
         pCleans += a.replaceAll("'", "").replaceAll("\"","")
       }
      var pCleansList:List[String] = pCleans.toList;
      if (p.length == 9) {
        val row: Row = Row(pCleansList(0), pCleansList(1), pCleansList(2), pCleansList(3), pCleansList(4), pCleansList(5), pCleansList(6), pCleansList(7), pCleansList(8))
        row
      } else {
        Row("", "", "", "", "", "", "", "", "")
      }
    })
    // Apply the schema to the RDD.
    val base_df = sqlContext.createDataFrame(rowRDD, leadHistrySchema)
    base_df.show()
    base_df
  }     
   
       def getNaspDF(sqlContext:SQLContext,sc:SparkContext ): DataFrame = {
    var csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/All_People.csv"
    var rawTextRDD = sc.textFile(csvInPath)
    rawTextRDD = rawTextRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    val SchemaFields = "Last Name,First Name,Email Address,Updated At,NMC_NAME,Phone Number,Person Status,Person Score,Person Source,SFDC Type,NASP ID,Marketo SFDC ID,Id".split(",")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val leadHistrySchema = StructType(SchemaFields)
    val rowRDD = rawTextRDD.map(_.split(",")).map(p => {
       var pCleans = ListBuffer[String]()
       for (a <- p){
         pCleans += a.replaceAll("'", "").replaceAll("\"","")
       }
      var pCleansList:List[String] = pCleans.toList;
      if (p.length == 13) {
        //val row: Row = Row.fromSeq(pCleansList)
        val row: Row = Row(pCleansList(0), pCleansList(1), pCleansList(2), pCleansList(3), pCleansList(4), pCleansList(5), pCleansList(6), pCleansList(7), pCleansList(8), pCleansList(9), pCleansList(10), pCleansList(11), pCleansList(12))
        row
      } else {
        Row("", "", "", "", "", "", "", "", "", "", "", "", "")
      }
    })
    // Apply the schema to the RDD.
    val base_df = sqlContext.createDataFrame(rowRDD, leadHistrySchema)
    base_df.show(100)
    base_df
  }  
       
def getHistoryDF(sqlContext:SQLContext,sc:SparkContext): DataFrame = {
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
   
	def main(arg: Array[String]) {

	
	 	val isServer = true;
		if (isServer == true) {
			System.setProperty("hadoop.home.dir", "/opt/mapr/hadoop/hadoop-2.5.1/etc/hadoop"); //For server run 
		} else {
			System.setProperty("hadoop.home.dir", "C:\\winutils");  //For local run    
		}
		    
			  val jobName = "leadreport"
				var conf = new SparkConf().setMaster("local[2]").setAppName(jobName)
				
				var csvInPath_leadhist = "C:/SparkAnalytics/Marketing_Project/in/LeadHistory2017_pipe.csv"
				var csvInPath_activity = "C:/SparkAnalytics/Marketing_Project/in/MarketoActivityReport_Since_May.csv"
				var csvInPath_lead = "C:/SparkAnalytics/Marketing_Project/in/one_lead_09212017.csv"
				var csvInPath_campaign_type = "C:/SparkAnalytics/Marketing_Project/in/Campaign_type_mapping_dummy.csv"

				var outFileName_inq = DateTimeFormat.forPattern("YYYY_MM_dd").print(System.currentTimeMillis) + "_MARKETING_INQ"
        var outputfileLoc_inq = "C:/SparkAnalytics/Marketing_Project/out/" + outFileName_inq
				var outFileName_mql = DateTimeFormat.forPattern("YYYY_MM_dd").print(System.currentTimeMillis) + "_MARKETING_MQL"
        var outputfileLoc_mql = "C:/SparkAnalytics/Marketing_Project/out/" + outFileName_mql
				var outFileName_sal = DateTimeFormat.forPattern("YYYY_MM_dd").print(System.currentTimeMillis) + "_MARKETING_SAL"
        var outputfileLoc_sal = "C:/SparkAnalytics/Marketing_Project/out/" + outFileName_sal
        var outFileName_sql = DateTimeFormat.forPattern("YYYY_MM_dd").print(System.currentTimeMillis) + "_MARKETING_SQL"
        var outputfileLoc_sql = "C:/SparkAnalytics/Marketing_Project/out/" + outFileName_sql
        var outFileName_win = DateTimeFormat.forPattern("YYYY_MM_dd").print(System.currentTimeMillis) + "_MARKETING_WIN"
        var outputfileLoc_win = "C:/SparkAnalytics/Marketing_Project/out/" + outFileName_win
        var outFileName_cpl = DateTimeFormat.forPattern("YYYY_MM_dd").print(System.currentTimeMillis) + "_MARKETING_CPL"
        var outputfileLoc_cpl = "C:/SparkAnalytics/Marketing_Project/out/" + outFileName_cpl
        var outFileName_cpa = DateTimeFormat.forPattern("YYYY_MM_dd").print(System.currentTimeMillis) + "_MARKETING_CPA"
        var outputfileLoc_cpa = "C:/SparkAnalytics/Marketing_Project/out/" + outFileName_cpa        
        
        
        if (isServer == true) {
					    //outputfileLoc_inq = "/mapr/my.cluster.com/workflow/MARKETING" + outFileName_inq
							//csvInPath_leadhist = "/mapr/my.cluster.com/workflow/MARKETING/Lead_History_2017.csv"
              //csvInPath_emailactivity = "C:/SparkAnalytics/Marketing_Project/OpenEmailLeadActivities 2017-09-15_Since August.csv"							
							conf = new SparkConf().setAppName(jobName)
							
				 outFileName_inq = DateTimeFormat.forPattern("YYYY_MM_dd").print(System.currentTimeMillis) + "_MARKETING_INQ"
         outputfileLoc_inq = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/out/MKmetrics/" + outFileName_inq
				 outFileName_mql = DateTimeFormat.forPattern("YYYY_MM_dd").print(System.currentTimeMillis) + "_MARKETING_MQL"
         outputfileLoc_mql = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/out/MKmetrics/" + outFileName_mql
				 outFileName_sal = DateTimeFormat.forPattern("YYYY_MM_dd").print(System.currentTimeMillis) + "_MARKETING_SAL"
         outputfileLoc_sal = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/out/MKmetrics/" + outFileName_sal
         outFileName_sql = DateTimeFormat.forPattern("YYYY_MM_dd").print(System.currentTimeMillis) + "_MARKETING_SQL"
         outputfileLoc_sql = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/out/MKmetrics/" + outFileName_sql
         outFileName_win = DateTimeFormat.forPattern("YYYY_MM_dd").print(System.currentTimeMillis) + "_MARKETING_WIN"
         outputfileLoc_win = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/out/MKmetrics/" + outFileName_win
         outFileName_cpl = DateTimeFormat.forPattern("YYYY_MM_dd").print(System.currentTimeMillis) + "_MARKETING_CPL"
         outputfileLoc_cpl = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/out/MKmetrics/" + outFileName_cpl
         outFileName_cpa = DateTimeFormat.forPattern("YYYY_MM_dd").print(System.currentTimeMillis) + "_MARKETING_CPA"
         outputfileLoc_cpa = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/out/MKmetrics/" + outFileName_cpa
				} else {
					    conf = new SparkConf().setMaster("local[2]").setAppName(jobName)
					     Logger.getLogger("org").setLevel(Level.OFF)
               Logger.getLogger("akka").setLevel(Level.OFF)					    
				}
			  
					     Logger.getLogger("org").setLevel(Level.OFF)
               Logger.getLogger("akka").setLevel(Level.OFF)	
               
        val sc = new SparkContext(conf)
				val sqlContext = new SQLContext(sc)	
        import sqlContext.implicits._
        var base_df_leadhist = getLeadHistoryDF(sqlContext,sc);
        base_df_leadhist.show()
        
//        var base_df_leadhist = sqlContext.read.format("com.databricks.spark.csv")
//				.option("header", "true") // Use first line of all files as header
//				.option("inferSchema", "true") // Automatically infer data types
//				.option("delimiter", "|")
//				.load(csvInPath_leadhist) 			  
				
				base_df_leadhist = base_df_leadhist.filter("Field = 'Status'")
				base_df_leadhist.show()
				
				base_df_leadhist.select("NewValue").distinct.show(100)
				//println(base_df_leadhist.count())
				//base_df_leadhist.show()
		  var market_users = List("005U0000006v1zP","0050P000006vp0gQAA","0050P000006lGvtQAE","0050P000006vp0bQAA","0050P000007MTR0QAO","00GU0000002chnPMAQ")
		  def isSALudf = udf((user: String) => {   
		    if (market_users.contains(user)) "N"
		    else "Y"  
		  })	
		  
		  base_df_leadhist = base_df_leadhist.withColumn("isSAL", isSALudf(base_df_leadhist("CreatedById")))
				var mql_lead_hist_df = base_df_leadhist.filter("NewValue = 'Marketing Qualified Lead' or NewValue = 'Connected-Still Qualifying' or NewValue = 'Outreach Made-Pending Response' or NewValue = 'Qualified' or NewValue = 'Disqualified'")
				mql_lead_hist_df.show()
				mql_lead_hist_df = mql_lead_hist_df.drop("CreatedById")
				                                   .drop("Field")
				                                   .drop("Id")
				                                   .drop("OldValue")
				
				                                 
				
				//var sal_lead_hist_df = base_df_leadhist.filter("NewValue = 'Connected-Still Qualifying' or NewValue = 'Outreach Made-Pending Response' or NewValue = 'Qualified' or NewValue = 'Disqualified'")
				//sal_lead_hist_df.show()
				var sal_lead_hist_df = base_df_leadhist.filter("isSAL = 'Y'")
				sal_lead_hist_df = sal_lead_hist_df.dropDuplicates(Seq("LeadId"))
				sal_lead_hist_df = sal_lead_hist_df.drop("CreatedById")
				                                   .drop("Field")
				                                   .drop("Id")
				                                   .drop("OldValue")
				                                   
				var sql_lead_hist_df = base_df_leadhist.filter("NewValue = 'Qualified'")
				//sal_lead_hist_df.show()
				sql_lead_hist_df = sql_lead_hist_df.drop("CreatedById")
				                                   .drop("Field")
				                                   .drop("Id")
				                                   .drop("OldValue") 

				                                   

				def udfToTime_hist = udf((timeStamp: String) => {  
				  				  if (timeStamp == null || timeStamp == "CreatedDate" || timeStamp.isEmpty()) {
        0
      }
      else {
					val inputFormat = new SimpleDateFormat("MM/dd/yyyy' 'HH:mm:ss")
							var date = inputFormat.parse(timeStamp) 
							date.getTime() 
      }
				})				

				
				mql_lead_hist_df = mql_lead_hist_df.withColumn("changedatesec", udfToTime_hist(mql_lead_hist_df("CreatedDate")))
				mql_lead_hist_df = mql_lead_hist_df.groupBy("LeadId").agg(min("changedatesec"))

				
				sal_lead_hist_df = sal_lead_hist_df.withColumn("changedatesec", udfToTime_hist(sal_lead_hist_df("CreatedDate")))
				sal_lead_hist_df = sal_lead_hist_df.groupBy("LeadId").agg(min("changedatesec"))
				
				sql_lead_hist_df = sql_lead_hist_df.withColumn("changedatesec", udfToTime_hist(sql_lead_hist_df("CreatedDate")))
				sql_lead_hist_df = sql_lead_hist_df.groupBy("LeadId").agg(min("changedatesec"))
				
				/*
				var base_df_activity = sqlContext.read.format("com.databricks.spark.csv")
				.option("header", "true") // Use first line of all files as header
				.option("inferSchema", "true") // Automatically infer data types
				.option("delimiter", ",")
				.load(csvInPath_activity) 			  
				*/
				var base_df_activity = getActivityDF(sqlContext,sc);
				
				base_df_activity.show()
				println(base_df_activity.count())
				
				base_df_activity = base_df_activity.dropDuplicates()
				println(base_df_activity.count())
				base_df_activity = base_df_activity.withColumnRenamed("leadId","MarketoleadId")
				var base_df_nasp = getNaspDF(sqlContext,sc);
				/*var base_df_nasp = sqlContext.read.format("com.databricks.spark.csv")
				.option("header", "true") // Use first line of all files as header
				.option("inferSchema", "true") // Automatically infer data types
				//.option("delimiter", "|")
				.load(csvInPath_nasp) */
				
				base_df_nasp = base_df_nasp//.drop("Job Title")
//				                           .drop("Phone Number")
//				                           .drop("Person Status")
//				                           .drop("Person Score")
//				                           .drop("Person Source")
//				                           .drop("Updated At")
				                           .drop("SFDC Type")
				                           
				
				base_df_nasp = base_df_nasp.withColumnRenamed("Id","NaspLeadId")
				                           .withColumnRenamed("NASP ID", "NASP")
				                           .withColumnRenamed("Last Name", "LastName")
				                           .withColumnRenamed("First Name", "FirstName")
//				                           .withColumnRenamed("Company Name", "CompanyName")
				                           .withColumnRenamed("Email Address", "Email")
				                           .withColumnRenamed("Phone Number", "Phone_Number")
				                           .withColumnRenamed("Person Status", "Person_Status")
				                           .withColumnRenamed("Person Score", "Person_Score")
				                           .withColumnRenamed("Person Source", "Person_Source")
				                           .withColumnRenamed("Marketo SFDC ID", "SFDC_ContactId")
//				                           .withColumnRenamed("Updated At", "Updated_At")
				base_df_nasp = base_df_nasp.drop("leadStatus").drop("sfdcLeadId").drop("sfdcContactId")
				println("Join 1")
				println(base_df_activity.count())
				println(base_df_nasp.count())
				base_df_activity = base_df_activity.join(base_df_nasp, base_df_activity("MarketoleadId") === base_df_nasp("NaspLeadId"), "left_outer" ) 
				println(base_df_activity.count())
				
//				var campaign_count_df = base_df_activity.groupBy("campaignId").count()
//				campaign_count_df.show()
				var base_df_lead = getLeadDF(sqlContext,sc);
        /*var base_df_lead = sqlContext.read.format("com.databricks.spark.csv")
				.option("header", "true") // Use first line of all files as header
				.option("inferSchema", "true") // Automatically infer data types
				//.option("delimiter", "|")
				.load(csvInPath_lead) */ 	
				
				base_df_lead = base_df_lead.drop("Owner.Name")
				base_df_lead.show()
				var campaign_df  = getCampDetailsDF(sqlContext,sc);
				var campaign_type_df =  getCampTypeDF(sqlContext,sc);
				campaign_df = campaign_df//.withColumn("camptimesec", udfToTime(campaign_df("createdAt")))
				                         .withColumnRenamed("id", "campaignId")
				
				/*                         
				println("Join 2")
				println(base_df_activity.count())
				println(campaign_df.count())
				base_df_activity = base_df_activity.join(campaign_df, "campaignId")				                         
				println(base_df_activity.count()) */
				
				
				
				def udfToTime = udf((timeStamp: String) => {  
				  if (timeStamp == null || timeStamp == "activityDate" || timeStamp.isEmpty()) {
        0
      }
      else {
					val inputFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
							var date = inputFormat.parse(timeStamp) 
							date.getTime() 
      }
				})
				
				def timeToStr = udf((milliSec: Long) => { 
			    DateTimeFormat.forPattern("MM/dd/YYYY").print(milliSec)  
		  })
		  


		  /* var max_camp_activity_df = base_df_activity.groupBy("MarketoleadId").agg(max("camptimesec"))
		  max_camp_activity_df = max_camp_activity_df.withColumnRenamed("MarketoleadId", "MarketoleadId_max")
				println("Join 3")
				println(base_df_activity.count())
				println(max_camp_activity_df.count())
		  
		  var lead_camp_df = base_df_activity.join(max_camp_activity_df, base_df_activity("MarketoleadId") === max_camp_activity_df("MarketoleadId_max") &&
		                                                                 base_df_activity("camptimesec") === max_camp_activity_df("max(camptimesec)"))
		                                                                 
		      println(lead_camp_df.count()) 
		      

		  
		                                                                 
      lead_camp_df = lead_camp_df.select("MarketoleadId","campaignId")
                                 //.withColumnRenamed("campaignId", "campaignId")
      
      lead_camp_df = lead_camp_df.dropDuplicates()
      
		  base_df_activity = base_df_activity.drop("campaignId") 
				println("Join 4")
				println(base_df_activity.count())
				println(lead_camp_df.count())		  
		  base_df_activity = base_df_activity.join(lead_camp_df, "MarketoleadId")
		  println(base_df_activity.count()) */
		  
				println("Join 2")
				println(base_df_activity.count())
				println(campaign_type_df.count())		  
		  base_df_activity = base_df_activity.join(campaign_type_df, "campaignId")
		  println(base_df_activity.count())
      base_df_activity.show()
				println("Join 3")
				println(base_df_activity.count())
				println(base_df_lead.count())      
				var activity_lead_df = base_df_activity.join(base_df_lead, base_df_activity("MarketoleadId") ===  base_df_lead("External_System_Lead_ID__c")) 
				println(activity_lead_df.count())
				
				activity_lead_df = activity_lead_df.withColumn("timestampsec", udfToTime(activity_lead_df("activityDate")))
				var max_activity_lead_df = activity_lead_df.groupBy("MarketoleadId").agg(max("timestampsec"))
				max_activity_lead_df = max_activity_lead_df.withColumnRenamed("MarketoleadId", "MarketoleadId_max")
			  println("Join 4")
				println(activity_lead_df.count())
				println(max_activity_lead_df.count())				
				activity_lead_df = activity_lead_df.join(max_activity_lead_df, activity_lead_df("MarketoleadId") === max_activity_lead_df("MarketoleadId_max") &&
				    activity_lead_df("timestampsec") === max_activity_lead_df("max(timestampsec)"))
				 println(activity_lead_df.count())
				activity_lead_df = activity_lead_df.drop("max(timestampsec)")
				                                   .drop("MarketoleadId_max")
			  println("Join 5")
				println(activity_lead_df.count())
				println(campaign_df.count())					                                   
				activity_lead_df = activity_lead_df.join(campaign_df, "campaignId")
				println(activity_lead_df.count())

				println("Join 6")
				println(base_df_activity.count())
				println(campaign_df.count())					                                   
				base_df_activity = base_df_activity.join(campaign_df, "campaignId")
				println(base_df_activity.count())
				
				base_df_activity = base_df_activity.withColumn("timestampsec", udfToTime(base_df_activity("activityDate")))
				base_df_activity = base_df_activity
				    .withColumn("EventType", lit("ROLLING"))
				    .withColumn("EventId", base_df_activity("id"))
				    //.withColumn("CampaignType", lit("Connectivity"))
				    .withColumn("EventName", lit("Inquiry"))
				    .withColumn("EventStartDate" ,timeToStr(base_df_activity("timestampsec")))
				    .withColumn("EventCompletionDate", lit(""))
				    .withColumn("Count", lit("1"))
				    .withColumn("SFDCLeadId", lit(""))
				    .withColumn("SFDCOppId", lit(""))
				    .withColumn("ActivityType", base_df_activity("activityTypeId"))
				
				    var inquiry_out_df = base_df_activity.select("SFDCLeadId","campaignId","CampaignType","SFDCCampaignID","MarketoleadId","ActivityType","SFDCOppId","NASP","FirstName","LastName","Email","Phone_Number","Person_Status","Person_Score","Person_Source","SFDC_ContactId","EventId","EventName","EventType","EventStartDate","EventCompletionDate","Count" )
				    inquiry_out_df.show()
            var head = "SFDCLeadId,campaignId,CampaignType,SFDCCampaignID,MarketoleadId,ActivityType,SFDCOppId,NASP,FirstName,LastName,Email,Phone_Number,Person_Status,Person_Score,Person_Source,SFDC_ContactId,EventId,EventName,EventType,EventStartDate,EventCompletionDate,Count" + System.lineSeparator
				    reportbase.writeFile(inquiry_out_df, "INQ", head)				    
				    println(outputfileLoc_inq)
				 /*inquiry_out_df.write.format("com.databricks.spark.csv")
				.mode("overwrite")
				.option("header", "false")
				.option("treatEmptyValuesAsNulls", "true") // No effect.
				.save(outputfileLoc_inq)*/
				
			//	var activity_lead_df = base_df_activity.join(base_df_lead, )
				println("Join 7")
				println(mql_lead_hist_df.count())
				println(activity_lead_df.count()) 				
				var mql_activity_lead_df =  mql_lead_hist_df.join(activity_lead_df, mql_lead_hist_df("LeadId") === activity_lead_df("Id"))
				println(mql_activity_lead_df.count())
				//  cost per lead calculation  
				mql_activity_lead_df = mql_activity_lead_df
				    .withColumn("CampDate", mql_activity_lead_df("createdAt").substr(1, 10))
				 
				 var cost_per_lead_df = mql_activity_lead_df.groupBy("campaignId").agg(count("MarketoleadId"))
				 //cost_per_lead_df = cost_per_lead_df.withColumn("MQLcost", cost_per_lead_df("CampaignCost") / cost_per_lead_df("count(MarketoleadId)"))
				println("Join 8")
				println(mql_activity_lead_df.count())
				println(cost_per_lead_df.count()) 				
				 mql_activity_lead_df = mql_activity_lead_df.join(cost_per_lead_df, "campaignId")		
				 println(mql_activity_lead_df.count())
				 
				 mql_activity_lead_df = mql_activity_lead_df
				    .withColumn("EventType", lit("ROLLING"))
				    .withColumn("EventId", mql_activity_lead_df("LeadId"))
				    //.withColumn("CampaignType", lit("Connectivity"))
				    .withColumn("EventName", lit("MQL"))
				    .withColumn("EventStartDate" ,timeToStr(mql_activity_lead_df("min(changedatesec)")))
				    .withColumn("EventCompletionDate", lit(""))
				    .withColumn("Count", lit("1"))
				    .withColumn("SFDCLeadId", mql_activity_lead_df("SFDC_Lead_ID__c"))
				    .withColumn("SFDCOppId", mql_activity_lead_df("SFDC_Opportunity_ID__c"))
				    .withColumn("ActivityType", lit(""))
				    .withColumn("MQLcost", mql_activity_lead_df("CampaignCost") / mql_activity_lead_df("count(MarketoleadId)"))
				    
				 var mql_out_df = mql_activity_lead_df.select("SFDCLeadId","campaignId","CampaignType","CampDate","MQLcost","SFDCCampaignID","MarketoleadId","ActivityType","SFDCOppId","NASP","FirstName","LastName","Email","Phone_Number","Person_Status","Person_Score","Person_Source","SFDC_ContactId","EventId","EventName","EventType","EventStartDate","EventCompletionDate","Count" )
				    mql_out_df.show()
				    head = "SFDCLeadId,campaignId,CampaignType,CampDate,MQLcost,SFDCCampaignID,MarketoleadId,ActivityType,SFDCOppId,NASP,FirstName,LastName,Email,Phone_Number,Person_Status,Person_Score,Person_Source,SFDC_ContactId,EventId,EventName,EventType,EventStartDate,EventCompletionDate,Count" + System.lineSeparator
				    reportbase.writeFile(mql_out_df, "MQL", head)
				    /*mql_out_df.write.format("com.databricks.spark.csv")
				      .mode("overwrite")
				      .option("header", "false")
				      .option("treatEmptyValuesAsNulls", "true") // No effect.
				      .save(outputfileLoc_mql)*/
				println("Join 9")
				println(sal_lead_hist_df.count())
				println(activity_lead_df.count())
					var sal_activity_lead_df =  sal_lead_hist_df.join(activity_lead_df, sal_lead_hist_df("LeadId") === activity_lead_df("Id"))
					println(sal_activity_lead_df.count())
					 sal_activity_lead_df = sal_activity_lead_df
				      .withColumn("CampDate", sal_activity_lead_df("createdAt").substr(1, 10))
				 
				   cost_per_lead_df = sal_activity_lead_df.groupBy("campaignId").agg(count("MarketoleadId"))
				println("Join 10")
				println(sal_activity_lead_df.count())
				println(cost_per_lead_df.count())
				   sal_activity_lead_df = sal_activity_lead_df.join(cost_per_lead_df, "campaignId")
				println(sal_activity_lead_df.count())   
					sal_activity_lead_df = sal_activity_lead_df
				    .withColumn("EventType", lit("ROLLING"))
				    .withColumn("EventId", sal_activity_lead_df("LeadId"))
				    //.withColumn("CampaignType", lit("Connectivity"))
				    .withColumn("EventName", lit("SAL"))
				    .withColumn("EventStartDate" ,timeToStr(sal_activity_lead_df("min(changedatesec)")))
				    .withColumn("EventCompletionDate", lit(""))
				    .withColumn("Count", lit("1"))
				    .withColumn("SFDCLeadId", sal_activity_lead_df("SFDC_Lead_ID__c"))
				    .withColumn("SFDCOppId", sal_activity_lead_df("SFDC_Opportunity_ID__c"))
				    .withColumn("SALcost", sal_activity_lead_df("CampaignCost") / sal_activity_lead_df("count(MarketoleadId)"))
				    .withColumn("ActivityType", lit(""))
				    
				 var sal_out_df = sal_activity_lead_df.select("SFDCLeadId","campaignId","CampaignType","CampDate","SALcost","SFDCCampaignID","MarketoleadId","ActivityType","SFDCOppId","NASP","FirstName","LastName","Email","Phone_Number","Person_Status","Person_Score","Person_Source","SFDC_ContactId","EventId","EventName","EventType","EventStartDate","EventCompletionDate","Count" )
				    sal_out_df.show()
				    head = "SFDCLeadId,campaignId,CampaignType,CampDate,SALcost,SFDCCampaignID,MarketoleadId,ActivityType,SFDCOppId,NASP,FirstName,LastName,Email,Phone_Number,Person_Status,Person_Score,Person_Source,SFDC_ContactId,EventId,EventName,EventType,EventStartDate,EventCompletionDate,Count" + System.lineSeparator
				    reportbase.writeFile(sal_out_df, "SAL", head)				    
				    /*sal_out_df.write.format("com.databricks.spark.csv")
				      .mode("overwrite")
				      .option("header", "false")
				      .option("treatEmptyValuesAsNulls", "true") // No effect.
				      .save(outputfileLoc_sal)*/
					


				println("Join 11")
				println(sql_lead_hist_df.count())
				println(activity_lead_df.count())
				
				 var sql_activity_lead_df =  sql_lead_hist_df.join(activity_lead_df, sql_lead_hist_df("LeadId") === activity_lead_df("Id"))
				 println(sql_activity_lead_df.count())  
				 sql_activity_lead_df = sql_activity_lead_df
				      .withColumn("CampDate", sql_activity_lead_df("createdAt").substr(1, 10))
				 
				   cost_per_lead_df = sql_activity_lead_df.groupBy("campaignId").agg(count("MarketoleadId"))
				   //cost_per_lead_df = cost_per_lead_df.withColumn("SQLcost", cost_per_lead_df("CampaignCost") / cost_per_lead_df("count(MarketoleadId)"))
				println("Join 12")
				println(sql_activity_lead_df.count())
				println(cost_per_lead_df.count())				   
				   sql_activity_lead_df = sql_activity_lead_df.join(cost_per_lead_df, "campaignId")		
						println(sql_activity_lead_df.count())
				   sql_activity_lead_df = sql_activity_lead_df
				    .withColumn("EventType", lit("ROLLING"))
				    .withColumn("EventId", sql_activity_lead_df("LeadId"))
				    //.withColumn("CampaignType", lit("Connectivity"))
				    .withColumn("EventName", lit("SQL"))
				    .withColumn("EventStartDate" ,timeToStr(sql_activity_lead_df("min(changedatesec)")))
				    .withColumn("EventCompletionDate", lit(""))
				    .withColumn("Count", lit("1"))
				    .withColumn("SFDCLeadId", sql_activity_lead_df("SFDC_Lead_ID__c"))
				    .withColumn("SFDCOppId", sql_activity_lead_df("SFDC_Opportunity_ID__c"))
				    .withColumn("ActivityType", lit(""))
				    .withColumn("SQLcost", sql_activity_lead_df("CampaignCost") / sql_activity_lead_df("count(MarketoleadId)"))
				    
				 var sql_out_df = sql_activity_lead_df.select("SFDCLeadId","campaignId","CampaignType","CampDate","SQLcost","SFDCCampaignID","MarketoleadId","ActivityType","SFDCOppId","NASP","FirstName","LastName","Email","Phone_Number","Person_Status","Person_Score","Person_Source","SFDC_ContactId","EventId","EventName","EventType","EventStartDate","EventCompletionDate","Count" )
				    sql_out_df.show()
				    head = "SFDCLeadId,campaignId,CampaignType,CampDate,SQLcost,SFDCCampaignID,MarketoleadId,ActivityType,SFDCOppId,NASP,FirstName,LastName,Email,Phone_Number,Person_Status,Person_Score,Person_Source,SFDC_ContactId,EventId,EventName,EventType,EventStartDate,EventCompletionDate,Count" + System.lineSeparator
				    reportbase.writeFile(sql_out_df, "SQL", head)
				    /*sql_out_df.write.format("com.databricks.spark.csv")
				      .mode("overwrite")
				      .option("header", "false")
				      .option("treatEmptyValuesAsNulls", "true") // No effect.
				      .save(outputfileLoc_sql)*/
				      
				  var csvInPath_opp = "C:/SparkAnalytics/in/cx_opptyhist_oppty_sfdc.txt"
				  var base_df_opp = getHistoryDF(sqlContext,sc)
				  /*sqlContext.read.format("com.databricks.spark.csv")
				      .option("header", "true") // Use first line of all files as header
				      .option("inferSchema", "true") // Automatically infer data types
				      .option("delimiter", "|")
				      .load(csvInPath_opp) */
				  
				  base_df_opp = base_df_opp.withColumn("timestampsec_opp", udfToTime_hist(base_df_opp("CREATEDDATE")))
				  //base_df_opp = base_df_opp.filter("StageName = '5 Closed Won'")
				  base_df_opp = base_df_opp.drop("CREATEDDATE")
				  
				 var OppMax_df = base_df_opp.groupBy("SFDC_OPPORTUNITY_ID__C").agg(max("timestampsec_opp"))
				OppMax_df = OppMax_df.withColumnRenamed("SFDC_OPPORTUNITY_ID__C" , "SFDC_OPPORTUNITY_ID")	
				println("Join 13")
				println(base_df_opp.count())
				println(OppMax_df.count())
				
				var OppStage_df = base_df_opp.join(
				    OppMax_df, 
				    base_df_opp("SFDC_OPPORTUNITY_ID__C") === OppMax_df("SFDC_OPPORTUNITY_ID")
		      && base_df_opp("timestampsec_opp") === OppMax_df("max(timestampsec_opp)"))
				  println(OppStage_df.count())
		      OppStage_df = OppStage_df.filter("STAGENAME = '5 Closed Won'")
				 println("Join 14")
				println(sql_activity_lead_df.count())
				println(OppStage_df.count())
					 var win_activity_lead_df =  sql_activity_lead_df.join(OppStage_df, sql_activity_lead_df("SFDC_Opportunity_ID__c") === OppStage_df("SFDC_OPPORTUNITY_ID"))
						println(win_activity_lead_df.count())
					 win_activity_lead_df = win_activity_lead_df
				    .withColumn("EventType", lit("ROLLING"))
				    .withColumn("EventId", win_activity_lead_df("LeadId"))
				    //.withColumn("CampaignType", lit("Connectivity"))
				    .withColumn("EventName", lit("WIN"))
				    .withColumn("EventStartDate" ,timeToStr(win_activity_lead_df("min(changedatesec)")))
				    .withColumn("EventCompletionDate", lit(""))
				    .withColumn("Count", lit("1"))
				    .withColumn("SFDCLeadId", win_activity_lead_df("SFDC_Lead_ID__c"))
				    .withColumn("SFDCOppId", win_activity_lead_df("SFDC_Opportunity_ID__c"))
				    .withColumn("ActivityType", lit(""))
				    
				 var win_out_df = win_activity_lead_df.select("SFDCLeadId","campaignId","CampaignType","SFDCCampaignID","MarketoleadId","ActivityType","SFDCOppId","NASP","FirstName","LastName","Email","Phone_Number","Person_Status","Person_Score","Person_Source","SFDC_ContactId","EventId","EventName","EventType","EventStartDate","EventCompletionDate","Count" )
				    win_out_df.show()
				    head = "SFDCLeadId,campaignId,CampaignType,SFDCCampaignID,MarketoleadId,ActivityType,SFDCOppId,NASP,FirstName,LastName,Email,Phone_Number,Person_Status,Person_Score,Person_Source,SFDC_ContactId,EventId,EventName,EventType,EventStartDate,EventCompletionDate,Count" + System.lineSeparator
				    reportbase.writeFile(win_out_df, "WIN", head)
				    /*win_out_df.write.format("com.databricks.spark.csv")
				      .mode("overwrite")
				      .option("header", "false")
				      .option("treatEmptyValuesAsNulls", "true") // No effect.
				      .save(outputfileLoc_win)*/		
				      
				      /****************************/
				      // Cost per lead YTD        //
				      /****************************/
				      
				     /* mql_activity_lead_df = mql_activity_lead_df
				    .withColumn("CampDate", mql_activity_lead_df("createdAt").substr(1, 10))
				     var cost_per_lead_df = mql_activity_lead_df.groupBy("CampMonth").agg(count("MarketoleadId")).agg(sum("CampaignCost"))
				     cost_per_lead_df = cost_per_lead_df.withColumn("MQLcost", cost_per_lead_df("sum(CampaignCost)") / cost_per_lead_df("count(MarketoleadId)"))
				     cost_per_lead_df.write.format("com.databricks.spark.csv")
				      .mode("overwrite")
				      .option("header", "false")
				      .option("treatEmptyValuesAsNulls", "true") // No effect.
				      .save(outputfileLoc_cpl)
				      * 
				      */
				     
				      /****************************/
				      // Cost per Appointment YTD        //
				      /****************************/
				     /* 
				      sql_activity_lead_df = sql_activity_lead_df
				    .withColumn("CampMonth", sql_activity_lead_df("createdAt").substr(6, 2))
				     var cost_per_appt_df = sql_activity_lead_df.groupBy("CampMonth").agg(count("MarketoleadId")).agg(sum("CampaignCost"))
				     cost_per_appt_df = cost_per_appt_df.withColumn("SQLcost", cost_per_appt_df("sum(CampaignCost)") / cost_per_appt_df("count(MarketoleadId)"))
				     cost_per_lead_df.write.format("com.databricks.spark.csv")
				      .mode("overwrite")
				      .option("header", "false")
				      .option("treatEmptyValuesAsNulls", "true") // No effect.
				      .save(outputfileLoc_cpa)
				      * 
				      */
				      
				      
				     				      
				     
	}         
}



