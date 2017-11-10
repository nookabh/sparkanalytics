package com.workflow.mapexamples

import scala.io.Source
import java.io.File
import java.io.PrintWriter

import java.io.PrintWriter

object WCReadWriteHadoop {
  def main(arg: Array[String]) {
    for (i <- 1 to 100000000) {
      println(i)
       for (i <- 1 to 100000000) {
        println(i)
       }
    }

      //    println(Source.fromFile("C:/Users/nookabh/Desktop/json/data.txt")) // returns scala.io.BufferedSource non-empty iterator instance

      println(Source.fromFile("/home/z977833/mapr/demo.mapr.com/input/data.txt")) // returns scala.io.BufferedSource non-empty iterator instance

      //     val s1 = Source.fromFile("C:/Users/nookabh/Desktop/json/data.txt").mkString; //returns the file data as String
      val s1 = Source.fromFile("/home/z977833/mapr/demo.mapr.com/input/data.txt").mkString; //returns the file data as String
      println(s1)

      //splitting String data with white space and calculating the number of occurrence of each word in the file  
      val counts = s1.split("\\s+").groupBy(x => x).mapValues(x => x.length)

      println(counts)

      println("Count of JournalDev word:" + counts("JournalDev"))

      val writer = new PrintWriter(new File("/home/z977833/mapr/demo.mapr.com/input/Write.txt"))

      writer.write("Count of JournalDev word:" + counts)
      writer.close()

      Source.fromFile("/home/z977833/mapr/demo.mapr.com/input/Write.txt").foreach { x => print(x) }
  }

}