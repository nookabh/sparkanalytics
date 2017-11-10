package com.workflow.lead
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

object MKReportRunner  {
  
def main(arg: Array[String]) {
    //System.setProperty("hadoop.home.dir", "/etc/hadoop/conf");
      System.setProperty("hadoop.home.dir", "C:\\winutils");
    val isServer = false;
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
      conf = new SparkConf().setAppName(jobName).set("spark.driver.allowMultipleContexts", "true")
      csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/CX_PQ_Milestone_PP.csv"
      outputfileLoc = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/out/metrics/"
    } else {
      csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/testdata/PQ_CX_09212017.csv"
      conf = new SparkConf().setMaster("local[10]").setAppName(jobName).set("spark.executor.memory", "1g").set("spark.driver.allowMultipleContexts", "true")
    }
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    
    def getBaseDF(): DataFrame = {
      csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/CX_PQ_Milestone_PP.csv"
      val rawTextRDD = sc.textFile(csvInPath)
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
      val rawTextRDD = sc.textFile(csvInPath)
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
      val rawTextRDD = sc.textFile(csvInPath)
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
      val rawTextRDD = sc.textFile(csvInPath)
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
      val rawTextRDD = sc.textFile(csvInPath)
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
      val rawTextRDD = sc.textFile(csvInPath)
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
    
    try {
      remove(Paths.get("/appl_ware/dsdata/sfdccxdash/cxlanding/metrics/"))   //Remove all the old CSV files
      
      //Generating Metrics

    } finally {
      //sc.stop()
    }
  }  
}