package com.examples
/*
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import java.text.SimpleDateFormat

import scala.reflect.runtime.universe._
import scala.reflect.ClassTag
import java.util.ArrayList
import org.apache.solr.client.solrj.impl.HttpSolrClient
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.common.SolrInputDocument
import scala.tools.scalap.Decode
import com.google.gson.Gson

case class OpportunityStatus1_4(
  createdTimestamp: String,
  id: String,
  OpportunityId: String,
  StageName: String,
  SystemModstamp: String)
object WriteToSolr1_4 {
  def main(arg: Array[String]) {
    var logger = Logger.getLogger(this.getClass())
    val jobName = "OptyHistory"
    val conf = new SparkConf().setMaster("local[2]").setAppName(jobName)
    val sc = new SparkContext(conf)
    val test = "";
    logger.info("Opty History analysis");
    logger.info("=> jobName \"" + jobName + "\"")

    val solrC = new CloudSolrClient.Builder().withZkHost("gchsc1lna062.itcent.ebiz.verizon.com:6988").build()
    solrC.setDefaultCollection("spark-solr-workflow")
    val sQuery = new SolrQuery()
    val gson = new Gson()
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val csvInPath = "c:/ac/TestData/scala/test1.csv"
    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(csvInPath)
    df.show()

    df.collect().map(t => {
      val obj: OpportunityStatus1_4 = OpportunityStatus1_4(t.getAs("createdTimestamp"), t.getAs("id"), t.getAs("OpportunityId"), t.getAs("StageName"), t.getAs("SystemModstamp"))
      val jsonString = gson.toJson(obj)
      println(jsonString)
      val query = "id:\"" + t.getAs("id") + "\""
      sQuery.setQuery(query);
      val resp = solrC.query(sQuery)
      val json = gson.toJson(resp.getResults().get(0))

      val map = gson.fromJson(json, classOf[java.util.Map[String, String]])
      val document = new SolrInputDocument()
      document.addField("createdTimestamp", if (t.getAs("createdTimestamp") != null) t.getAs("createdTimestamp") else map.get("createdTimestamp"))
      document.addField("id", if (t.getAs("id") != null) t.getAs("id") else map.get("id"))
      document.addField("OpportunityId", if (t.getAs("OpportunityId") != null) t.getAs("OpportunityId") else map.get("OpportunityId"))
      document.addField("StageName", if (t.getAs("StageName") != null) t.getAs("StageName") else map.get("StageName"))
      document.addField("SystemModstamp", if (t.getAs("SystemModstamp") != null) t.getAs("SystemModstamp") else map.get("SystemModstamp"))
      val upResponse = solrC.add(document);
      solrC.commit();
      logger.info("=> Response \"" + upResponse + "\"")
     
    })

  }
}
*/