package com.workflow.cx

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import java.util.Calendar
import java.text.SimpleDateFormat
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import java.nio.file.{Files, Paths, Path, SimpleFileVisitor, FileVisitResult}
import java.nio.file.attribute.BasicFileAttributes
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.log4j.Level
object CXTransformationRunner {
  def main(arg: Array[String]) {
    System.setProperty("hadoop.home.dir", "/etc/hadoop/conf");
    //  System.setProperty("hadoop.home.dir", "C:\\winutils");
    val isServer = true;
    var logger = Logger.getLogger(this.getClass())
    var jobName = "TransformationMaster"
    var conf: SparkConf = null
    val now = Calendar.getInstance().getTime()
    val minuteFormat = new SimpleDateFormat("yyyy-MM-dd")
    val timestamp = minuteFormat.format(now)
    var csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/view_cx_opptyhist_oppty_sfdc.txt"
    var metricRunDate = DateTimeFormat.forPattern("YYYY_MM_dd").print(System.currentTimeMillis)
    var outputfileLoc = "C:/Users/nookabh/Desktop/json/"
    if (isServer == true) {
      conf = new SparkConf().setAppName(jobName).set("spark.driver.allowMultipleContexts", "true").set("spark.driver.maxResultSize", "5g")
      csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/CX_PQ_Milestone_PP.csv"
      outputfileLoc = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/out/metrics/"
    } else {
      conf = new SparkConf().setMaster("local[10]").setAppName(jobName).set("spark.executor.memory", "1g")
    }
    

    //Logger.getRootLogger().setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext: SQLContext = new HiveContext(sc)

    def getBaseDF(): DataFrame = {
      csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/CX_PQ_Milestone_PP.csv"
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
      base_df
    }

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

    def getTeamFormHistory(): DataFrame = {
      var csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/view_cx_tf_history_sfdc.txt"
      var rawTextRDD = sc.textFile(csvInPath)
      rawTextRDD = rawTextRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      val quoteHistorySchemaFields = "ID|PARENTID|FIELD|OLDVALUE|NEWVALUE|Team_Form_Hist_date|ES_OPPORTUNITY__C|SFDC_OPPORTUNITY_ID__C|Team_Form_Number|TEAM_INITIALS__C|ENGAGEMENT_SCRIPT__C|Team_Form_date|STATUS__C|ISACTIVE__C|VERSION_NO__C|STAGENAME".split("\\|")
        .map(fieldName => StructField(fieldName, StringType, nullable = true))
      val quoteHistorySchema = StructType(quoteHistorySchemaFields)
      val rowRDD = rawTextRDD.map(_.split("\\|")).map(p => {
        if (p.length == 16) {
          val row: Row = Row(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14), p(15))
          row
        } else {
          Row("", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "")
        }
      })
      // Apply the schema to the RDD.
      val teamformHis = sqlContext.createDataFrame(rowRDD, quoteHistorySchema)
      teamformHis
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
      quoteGSAT
    }
    
    def getOptyPQquote(): DataFrame = {
      var csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/view_cx_oppty_pq_quote.csv"
      var rawTextRDD = sc.textFile(csvInPath)
      rawTextRDD = rawTextRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      val OptyPQquoteSchemaFields = "OPPORTUNITY__C,QUOTE_ID,SFDC_OPPORTUNITY_ID__C".split(",")
        .map(fieldName => StructField(fieldName, StringType, nullable = true))
      val optyquoteSchema = StructType(OptyPQquoteSchemaFields)
      val rowRDD = rawTextRDD.map(_.split(",")).map(p => {
        if (p.length == 3) {
          val row: Row = Row(p(0), p(1), p(2))
          row
        } else {
          Row("", "", "")
        }
      })
      // Apply the schema to the RDD.
      val optypqquote = sqlContext.createDataFrame(rowRDD, optyquoteSchema)
      optypqquote
    }
    def getLineItemHistory(): DataFrame = {
      var csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/view_cx_oppty_lineitems.csv"
      var rawTextRDD = sc.textFile(csvInPath)
      rawTextRDD = rawTextRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      val OptyPQquoteSchemaFields = "ID,OPPORTUNITYID,SFDC_OPPORTUNITY_ID__C,PRM_PRODUCT_ROLLUP_1_PR1__C,PR2__C,PRODUCT_GROUP_PR3__C,PRODUCT_FAMILY_PR4__C,PRODUCT_DESIGNATOR__C".split(",")
        .map(fieldName => StructField(fieldName, StringType, nullable = true))
      val optyquoteSchema = StructType(OptyPQquoteSchemaFields)
      val rowRDD = rawTextRDD.map(_.split(",")).map(p => {
        if (p.length == 8 && p(2) != "SFDC_OPPORTUNITY_ID__C") {
            val row: Row = Row(p(0), p(1), p(2),p(3), p(4), p(5),p(6), p(7))
            row
        } else {
          Row("", "", "","", "", "","", "")
        }
      })
      // Apply the schema to the RDD.
      val optypqquote = sqlContext.createDataFrame(rowRDD, optyquoteSchema)
      optypqquote
    }
    
     def remove(root: Path): Unit = {
        Files.walkFileTree(root, new SimpleFileVisitor[Path] {
          override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
            Files.delete(file)
            FileVisitResult.CONTINUE
          }
        })
      }

    def getOwnerId(): DataFrame = {
      var csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/view_cx_oppty_owner.csv"
      val rawTextRDD = sc.textFile(csvInPath)
      val OwnerIdSchemaFields = "ID,SFDC_OPPORTUNITY_ID__C,OWNERID,NAME,VZ_ID__C".split(",")
        .map(fieldName => StructField(fieldName, StringType, nullable = true))
      val ownerSchema = StructType(OwnerIdSchemaFields)
      val rowRDD = rawTextRDD.map(_.split(",")).map(p => {
        if (p.length == 5) {
          val row: Row = Row(p(0), p(1), p(2), p(3), p(4))
          row
        } else {
          Row("", "", "", "", "")
        }
      })
      // Apply the schema to the RDD.
      val ownerresults = sqlContext.createDataFrame(rowRDD, ownerSchema)
      ownerresults
    }

    def getUsers(): DataFrame = {
      var csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/view_cx_user.txt"
      val rawTextRDD = sc.textFile(csvInPath)
      val userSchemaFields = "ENTERPRISE_ID__C|VZ_ID__C|EID_0__C|EID_10__C|EID_11__C|EID_12__C|EID_13__C|EID_14__C|EID_15__C|EID_1__C|EID_2__C|EID_3__C|EID_4__C|EID_5__C|EID_6__C|EID_7__C|EID_8__C|EID_9__C|NAME|ID".split("\\|")
        .map(fieldName => StructField(fieldName, StringType, nullable = false))
      val userSchema = StructType(userSchemaFields)
      val rowRDD = rawTextRDD.map(_.split("\\|")).map(p => {
        if (p.length == 20) {
          val row: Row = Row(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14), p(15), p(16), p(17), p(18), p(19))
          row
        } else {
          print(p.length)
          Row("", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "")
        }
      })
      // Apply the schema to the RDD.
      val userresults = sqlContext.createDataFrame(rowRDD, userSchema)
      userresults
    }

    def getSales_Directors(): DataFrame = {
      var csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/sales_directors.txt"
      val rawTextRDD = sc.textFile(csvInPath)
      val salesSchemaFields = "OWNERID|ISDELETED|NAME|CREATEDDATE|CREATEDBYID|LASTMODIFIEDDATE|LASTMODIFIEDBYID|SYSTEMMODSTAMP|LASTACTIVITYDATE|LASTVIEWEDDATE|LASTREFERENCEDDATE|ALLNASP_DIR_NAME__C|VZ_ID__C|GVP__C|RVP__C|SVP__C|GVP_USER__C|RVP_USER__C|SVP_USER__C|ZVP_USER__C|ID".split("\\|")
        .map(fieldName => StructField(fieldName, StringType, nullable = true))
      val salesdirSchema = StructType(salesSchemaFields)
      val rowRDD = rawTextRDD.map(_.split("\\|")).map(p => {
        if (p.length == 21) {
          val row: Row = Row(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(13), p(14), p(15), p(16), p(17), p(18), p(19), p(20))
          row
        } else {
          Row("", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "")
        }
      })
      // Apply the schema to the RDD.
      val salesdirresults = sqlContext.createDataFrame(rowRDD, salesdirSchema)
      salesdirresults
    }
    
    def getVISEHistory(): DataFrame = {
      var csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/view_cx_vise_history_sfdc.csv"
      var rawTextRDD = sc.textFile(csvInPath)
      rawTextRDD = rawTextRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      val SchemaFields = "ID,PARENTID,FIELD,OLDVALUE,NEWVALUE,VISE_HIST_CREATED_DATE,SFDC_OPPORTUNITY_ID__C,VISE_STATUS__C,VISE_DESIGN_APPLICATION_ID__C,VISE_CREATED_DATE".split(",")
        .map(fieldName => StructField(fieldName, StringType, nullable = true))
      val Schema = StructType(SchemaFields)
      val rowRDD = rawTextRDD.map(_.split(",")).map(p => {
        if (p.length == 10) {
          val row: Row = Row(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9))
          row
        } else {
          Row("", "", "", "", "", "", "", "", "", "")
        }
      })
      // Apply the schema to the RDD.
      val results = sqlContext.createDataFrame(rowRDD, Schema)
      results
    } 
    def getSitesDetails(): DataFrame = {
       //csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/CX_PQ_SITE_PROD_FEAT.csv"
       csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/CX_PQ_SITE.csv"
      var rawTextRDD = sc.textFile(csvInPath)
      rawTextRDD = rawTextRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
     // val OptyPQquoteSchemaFields = "QTE_ID|REVIS_ID|OPTY_ID|NASP_ID|GCH_ID|LGL_ENTY_CODE|SITE_ID|SITE_NAME|SITE_CREATED_DATE|addr_type_code|cntry_code|line_1_text|line_2_text|line_3_text|line_4_text|city_name|terr_code|postal_number|postal_ext_number".split("\\|")
      val OptyPQquoteSchemaFields = "QTE_ID|REVIS_ID|OPTY_ID|SITE_ID|SITE_NAME|SITE_CREATED_DATE".split("\\|")
        .map(fieldName => StructField(fieldName, StringType, nullable = true))
      val optyquoteSchema = StructType(OptyPQquoteSchemaFields)

      val rowRDD = rawTextRDD.map(_.split("\\|")).map(p => {
        if (p.length == 18) {
           val row: Row = Row(p(0), p(1), p(2), p(6), p(7),p(8))
          row
        } else {
          Row("", "", "", "", "","")
        }
        
          /*Row.fromSeq(p.toSeq)
        } else {
          Row("", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "")
        }*/
      })
      // Apply the schema to the RDD.
      val siteData = sqlContext.createDataFrame(rowRDD, optyquoteSchema)
      val siteDataNotNull = siteData.where(siteData("QTE_ID").notEqual("") && siteData("SITE_ID").notEqual(""))
      siteDataNotNull
    }
//    remove(Paths.get("/appl_ware/dsdata/sfdccxdash/cxlanding/metrics/"))   //Remove all the old CSV files
   def getGCTData(): DataFrame = {
      var csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/GCT_EXPORT_CXDASHBOARD_10192017.csv"
      //var csvInPath = "C:/ac/TestData/scala/in/Export_CXDashBoard.csv"
      var rawTextRDD = sc.textFile(csvInPath)
      rawTextRDD = rawTextRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      val OptyPQquoteSchemaFields = "QUOTE_ID,QUOTE_VERSION_ID,DOCID,EVENT,MODIFICATION_DATE,MODIFICATION".split(",")
        .map(fieldName => StructField(fieldName, StringType, nullable = true))
      val optyquoteSchema = StructType(OptyPQquoteSchemaFields)
      val rowRDD = rawTextRDD.map(_.split(",")).map(p => {
        if (p.length == 6) {
          val row: Row = Row(p(0).replace("\"", ""), p(1).replace("\"", ""), p(2).replace("\"", ""), p(3).replace("\"", ""), p(4).replace("\"", ""), p(5).replace("\"", ""))
          row
        } else {
          Row("", "", "", "", "", "")
        }
      })
      // Apply the schema to the RDD.
      val gctData = sqlContext.createDataFrame(rowRDD, optyquoteSchema)
      //gctData.show()
      gctData
    }
   
    def getOpptyAccountdf(): DataFrame = {
      var csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/view_cx_oppty_account_sfdc.txt"
      var rawTextRDD = sc.textFile(csvInPath)
      rawTextRDD = rawTextRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      val OptyPQquoteSchemaFields = "Oppty_id|Oppty_name|SFDC_OPPORTUNITY_ID__C|BOOKING__C|REGION__C|CREATEDDATE|QVIDIAN_PRODUCTS__C|OPPORTUNITY_OWNER_REGION__C|ACCOUNTID|STAGENAME|CLOSEDATE|ACCOUNT_NASP_NAME__C|GCH_ID__C|SFDC_ACCOUNT_ID__C|Account_name|NASP_NAME__C|OPPORTUNITY_OWNER_MANAGER__C|OWNER_MGR|DIRECTOR__C|GVP__C|ZVP_USER__C|ANSWER_10__C|OWNING_AREA__C|Segment".split("\\|")
        .map(fieldName => StructField(fieldName, StringType, nullable = true))
      val optyquoteSchema = StructType(OptyPQquoteSchemaFields)
      val rowRDD = rawTextRDD.map(_.split("\\|")).map(p => {
        if (p.length == 24) {
          Row.fromSeq(p.toSeq)
        } else {
          Row("", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "")
        }
      })
      // Apply the schema to the RDD.
      val getOptyData = sqlContext.createDataFrame(rowRDD, optyquoteSchema)
      getOptyData
    }

    try {
      //Generating CXMetrics
      //Initial Release METRICS
      val ApprovalTimeObj = new ApprovalTimeMetric(getBaseDF())
      val EvalTimeObj = new EvalTimeMetric(getBaseDF())
      val PCMEngagedQuoteObj = new PCMEngagedQuote(getBaseDF())
      val PriceContractSignObj = new PriceContractSignMetric(getBaseDF())
      val PriceNegObj = new PriceNegMetric(getBaseDF())
      val VPApprovalObj = new VPApprovalMetric(getBaseDF())
      val ProductsCountObj = new ProductsCount(getLineItemHistory())
      val ConfigTimeObj = new ConfigTime(getBaseDF())
      val ContractingTimeObj = new ContractingTime(getBaseDF())
      val ContractSignNewObj = new ContractSignNew(getBaseDF())
      val DAEngagedTimeObj = new DAEngagedTime(getTeamFormHistory())
      val ECSTimeObj = new ECSTime(getBaseDF())
      val InitialDraftContractObj = new InitialDraftContract(getBaseDF())
      val QuotePricedObj = new QuotePriced(getBaseDF())
      val WinRateObj = new WinRate(getHistoryDF())
      val OppCreateToDesignObj = new OppCreateToDesign(getHistoryDF())
      val OppStage0Obj = new OppStage0(getHistoryDF())
      val OppStage1Obj = new OppStage1(getHistoryDF())
      val OppDesignToQuoteObj = new OppDesignToQuote(getHistoryDF(),getBaseDF())
      val GSATMetricsObj = new GSATMetricsCalculator(getGSATdata())
      val OPP_VRDObj = new OPP_VRD(getBaseDF())
      val PPA_Obj = new PPAMetricCalculator(getHistoryDF(),getBaseDF() )
      val PRCuntsObj = new PRCount(getLineItemHistory(),sqlContext)
      
            //Second Release METRICS
     val QuotesPerMultiQuoteSetObj = new QuotesPerMultiQuoteSet(getBaseDF())
     val MultiQuoteSetsObj = new MultiQuoteSets(getBaseDF(),getHistoryDF())
     val QuotesPerOpportunityObj = new QuotesPerOpportunity(getBaseDF())
     val SitesPerMultiQuoteSet = new SitesPerMultiQuoteSet(getBaseDF(),getSitesDetails())
     val ContractExecutionObj = new ContractExecution(getGCTData(), getBaseDF())
     val FinalContractGenerationnObj = new FinalContractGeneration(getGCTData(), getBaseDF())
     val salesHire = new SalesHierarchyMetric(getOwnerId(), getUsers(), getSales_Directors(), sqlContext)
     val PCMEngagementFilterObj = new PCMEngagementFilter(getHistoryDF(),getTeamFormHistory(),getBaseDF()) //NVDW-455
     val VPApprovalVol = new VPApprovalVol(getBaseDF())
     val SupportResourcesFilter = new SupportResourcesFilter(getHistoryDF(),getGSATdata(),getTeamFormHistory(),getVISEHistory())
     val Stage2toVIDEDesignStartObj = new Stage2toVIDEDesignStart(getHistoryDF(), getVISEHistory()) //NVDW-481
     val DraftWithPricingVolObj = new DraftContractWithPricingVol(getOpptyAccountdf(),getBaseDF())
     val DraftWithoutPricingVolObj = new DraftContractWithoutPricingVol(getOpptyAccountdf(),getBaseDF())
      
    } finally {
      //sc.stop()
    }
  }
}