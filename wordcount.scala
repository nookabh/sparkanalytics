package com.workflow.batch.trails

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object wordcount {
  def main(arg: Array[String]) {
    var logger = Logger.getLogger(this.getClass())
    //System.setProperty("SPARK_YARN_MODE", "true")
    val jobName = "word count1"
     val conf = new SparkConf().setMaster("local[2]").setAppName(jobName)
    //val conf = new SparkConf().setAppName(jobName)
    //    val conf = new SparkConf().setMaster("yarn-client").setAppName(jobName)
    //    conf.set("spark.hadoop.dfs.nameservices", "113.132.171.230:8020");
    //    conf.set("spark.hadoop.yarn.resourcemanager.hostname", "113.132.171.230") 
    //    conf.set("spark.hadoop.yarn.resourcemanager.address", "113.132.171.230:8050")
    //    conf.set("spark.hadoop.yarn.application.classpath",
    //      "$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,"
    //        + "$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,"
    //        + "$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*,"
    //        + "$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*")
    //    //conf.set("");
    //    conf.setJars(List("/Users/v494907/Documents/hack/spark-scala-mvn-boilerplate/target/spark-scala-maven-project-0.0.1-SNAPSHOT-jar-with-dependencies.jar"))
    
    
    val sc = new SparkContext(conf)

    
    for (i <- 1 to 111111) {
      for (j <- 1 to 11) {
        println(i)
      }
    }

    
    val test = "";
    logger.info("word count analysis");
    logger.info("=> jobName \"" + jobName + "\"")
    //windows
    val lines = sc.textFile("/Users/v494907/Documents/hack/docker-spark/scripts/notebooks/Opty History/namestext.rtf", 1)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    val localValues = wordCounts.take(100)
    wordCounts.foreach(r => println(r))

  }
}