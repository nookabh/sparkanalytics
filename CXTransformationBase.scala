package com.workflow.cx

import java.io.File

import org.apache.spark.sql.DataFrame
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import java.io.BufferedOutputStream

class CXTransformationBase() {
  //System.setProperty("hadoop.home.dir", "C:\\winutils");
  def writeFile(outDF: DataFrame, metricName: String, columsHead: String) {
    val outputfileLoc = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/out/metrics/"
    //val outputfileLoc = "C:/ac/TestData/scala/out/metrics/"
    val outFileName = outputfileLoc + metricName
    println("Creating "+metricName+" files.")
    outDF //.coalesce()
      .write.format("com.databricks.spark.csv")
      .mode("overwrite") //.option("header", "true")
      .option("treatEmptyValuesAsNulls", "true")
      .save(outFileName)
    merge(metricName, columsHead)
  }
  def createheaderFile(column:String) {
    val path = new Path("hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/out/metrics/Inter/columnhead")
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    hadoopConfig.setInt("dfs.blocksize", 16 * 1024 * 1024) // 16MB HDFS Block Size
    val fs = path.getFileSystem(hadoopConfig)
    if (fs.exists(path))
      fs.delete(path, true)
    val out = new BufferedOutputStream(fs.create(path))
    val txt = "Some text to output"
    out.write(column.getBytes("UTF-8"))
    out.flush()
    out.close()
    //fs.close()
  }
  def merge(metricName: String, columsHead: String): Unit = {
    try {

      val outputfileLoc = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/out/metrics/"
      //val outputfileLoc = "C:/ac/TestData/scala/out/metrics/"
      val outFileName = outputfileLoc + metricName
      val outLoc = "hdfs://zdwttda10.ebiz.verizon.com:8020/user/sfdcuser/out/"
      //val outLoc = "C:/ac/TestData/scala/out/"
      val hadoopConfig = new Configuration()
      val hdfs = FileSystem.get(hadoopConfig)
      var newFilePath = outLoc + metricName+".csv"
      var output2 = new Path(outLoc + metricName + ".csv"): Path
      if(metricName == "OPP_PRODUCTS"){
        newFilePath = outLoc + metricName+".txt"
        output2 = new Path(outLoc + metricName + ".txt"): Path
      }        
     // println(newFilePath)
      
      if (hdfs.exists(output2)) {
        hdfs.delete(output2, true)
      }
      val output1 = new Path(outputfileLoc + "/Inter/data"): Path
      if (hdfs.exists(output1)) {
        hdfs.delete(output1, true);
      }
      createheaderFile(columsHead)
      
      /*import java.io._

      val otempFile = hdfs.create(new Path(outputfileLoc + "/Inter/columnhead"));
      val os = new BufferedOutputStream(otempFile)
      os.write(columsHead.getBytes("UTF-8"))
      os.close()*/

     // val tempData: List[String] = columsHead.split(",").map(_.trim).toList
      /*val file = outputfileLoc + "/Inter/columnhead"
      val data = columsHead.split("|").toList
      val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))

      for (x <- data) {
        if (x == data.last)
          writer.write(x + System.lineSeparator)
        else
          writer.write(x + "|")
      }
      writer.close()*/
      
      //FileUtil.fullyDelete(new File(outputfileLoc+"/Inter/data"))
      FileUtil.copyMerge(hdfs, new Path(outFileName + "/"), hdfs, new Path(outputfileLoc + "/Inter/data"), true, hadoopConfig, null)
      FileUtil.fullyDelete(new File(outputfileLoc + "/Inter/.data.crc"))
      FileUtil.copyMerge(hdfs, new Path(outputfileLoc + "/Inter/"), hdfs, new Path(newFilePath), false, hadoopConfig, null)
      FileUtil.fullyDelete(new File(outputfileLoc + "/Inter/data"))
      FileUtil.fullyDelete(new File(outputfileLoc+"/Inter/columnhead"))
      FileUtil.fullyDelete(new File(outputfileLoc+"/Inter/.columnhead.crc"))
      if(metricName == "OPP_PRODUCTS")
        FileUtil.fullyDelete(new File(outLoc + "." + metricName + ".txt.crc"))
      else          
        FileUtil.fullyDelete(new File(outLoc + "." + metricName + ".csv.crc"))
      println(newFilePath + " created.")
    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }

  }
}