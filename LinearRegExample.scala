package com.workflow.pipeline.example
import org.apache.spark.mllib.classification.{ SVMModel, SVMWithSGD }
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
 import scala.beans.BeanInfo
 import org.apache.spark.{SparkConf, SparkContext}
 import org.apache.spark.ml.Pipeline
 import org.apache.spark.ml.classification.LogisticRegression
 import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
 import org.apache.spark.mllib.linalg.Vector
 import org.apache.spark.sql.{Row, SQLContext}
 import org.apache.spark.sql.DataFrame
 import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType;
 
object LinearRegExample {
  def main(arg: Array[String]) {
     System.setProperty("hadoop.home.dir", "C:\\winutils");
    var jobName = "TransformationMaster"
    var conf: SparkConf = null
    conf = new SparkConf().setMaster("local[10]").setAppName(jobName).set("spark.executor.memory", "1g").set("spark.driver.allowMultipleContexts", "true")
    var csvInPath = ""
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // Load and parse the data file, converting it to a DataFrame.
    val data1 = sqlContext.read.format("libsvm").load("C:\\Users\\nookabh\\Desktop\\Jupyter_Notebook\\trainingdata\\sample_little_libsvm.txt")

    def getPricingDF(): DataFrame = {
      csvInPath = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/in/CX_PQ_PROD_FEAT_COMP.csv"
      var rawTextRDD = sc.textFile(csvInPath)
      rawTextRDD = rawTextRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      val quoteSchemaFields = "QTE_ID|REVIS_ID|quote_created_date|SITE_ID|SOLN_INST_ID|PROD_INST_ID|FEAT_INST_ID|addr_id|cntry_code|addr_type_code|line_1_text|line_2_text|line_3_text|line_4_text|city_name|terr_code|postal_number|postal_ext_number|PROD_CODE|USEC_Name|mlstn_name|PR1|PR2|PR3|PR4|MRC_AMT|NRC_AMT".split("\\|")
        .map(fieldName => StructField(fieldName, StringType, nullable = true))
      val quoteSchema = StructType(quoteSchemaFields)
      val rowRDD = rawTextRDD.map(_.split("\\|")).map(p => {
        if (p.length == 27) {
          Row.fromSeq(p.toSeq)
        } else {
          Row("", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "")
        }
      })
      var pricing_df = sqlContext.createDataFrame(rowRDD, quoteSchema)
      pricing_df
    }
//case class LabeledDocument(Userid: Double, Date: String, label: Double)
// val training = sqlContext.read.option("inferSchema", true).csv("/root/Predictiondata2.csv").toDF
// ("Userid","Date","label").toDF().as[LabeledDocument]
    
var training = getPricingDF()
 training  = training.select("PR4", "MRC_AMT", "postal_number", "cntry_code")
    training.show()
 val tokenizer = new Tokenizer().setInputCol("PR4").setOutputCol("words")
 val hashingTF = new HashingTF().setNumFeatures(1000).setInputCol(tokenizer.getOutputCol).setOutputCol("features")
 import org.apache.spark.ml.regression.LinearRegression
 val lr = new LinearRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
 val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))
 val model = pipeline.fit(training.toDF())
 training.show()
 case class Document(Userid: Integer, Date: String)
 val test = sc.parallelize(Seq(Document(4, "04-Jan-18"),Document(5, "01-Jan-17"),Document(2, "03-Jan-17")))
// model.transform(test.toDF()).show()
 
  }
}