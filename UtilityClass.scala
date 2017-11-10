package com.workflow.rt

import org.joda.time.format.DateTimeFormat
import java.text.SimpleDateFormat
import java.sql.Date
import java.time.DayOfWeek.SUNDAY;
import java.time.temporal.TemporalAdjusters.next;
import java.time.temporal.TemporalAdjusters.previous;
import scala.collection.mutable.ListBuffer
//import org.joda.time.DateTimeZone
//import org.joda.time.LocalDate
import java.util.Calendar
import java.util.GregorianCalendar


object UtilityClass {
  
  def main(arg: Array[String]) {
    var syndays = allSundaysOfAYear(1)
  }

  /*
   * Convert Milliseconds to number of days return the Counts, default value will be 1 day as count
   */
  def millisecondToDay(sec: Long): Long = {
    if (sec == 0) {
      return 1
    }
    val days = (sec / (1000 * 60 * 60 * 24));
    return days
  }

  /*
   * Convert this date MM/dd/YYYY to milliseconds 
   */
  def timeToStr(milliSec: Long): String =
    DateTimeFormat.forPattern("MM/dd/YYYY").print(milliSec)
  def timeStringToStr(milliSec: String): String =   
    DateTimeFormat.forPattern("MM/dd/YYYY").print(milliSec.toLong)
   /*
     * Covnert date from string format to MM/dd/yyyy
     */
  def findDiff(a: String): Long = {
    val inputFormat = new SimpleDateFormat("MM/dd/yyyy")
    if (a == null || a == "milestone_date" || a.isEmpty()) {
      return 0
    }
    val sdate = inputFormat.parse(a);
    return (sdate.getTime())
  }
  //anoop
  def minus(a: Long, b: Long): Long = {
    return (a - b)
  }
  def dateToMillSecond(a: String): Long = {
    //val inputFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    val inputFormat = new SimpleDateFormat("MM/dd/yyyy")
    if (a == null || !a.contains("/")) {
      return 0
    }
    val sdate = inputFormat.parse(a);
    //println(a+" ##"+sdate.getTime()+" ##"+sdate.getDate())
    
    return (sdate.getTime())

  }
   def fullDateToMillSecond(a: String): Long = {
   val inputFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss")
    if (a == null || !a.contains("/")) {
      return 0
    }
    val sdate = inputFormat.parse(a)
    return (sdate.getTime())

  }
   def dateToMillSecondSFDCStanadrad(a: String): Long = {
    val inputFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    //val inputFormat = new SimpleDateFormat("MM/dd/yyyy")
    if (a == null || !a.contains("/")) {
      return 0
    }
    //if(a.contains("/")){
      val sdate = inputFormat.parse(a);
      return (sdate.getTime())
    //}
   //return 0
    
  }
  def dateStrToDateTime(a: String): Date = {
    val inputFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss")
    if (a == null || !a.contains("/")) {
      return null
    }
    val sqlDate = new java.sql.Date(inputFormat.parse(a).getTime())
    sqlDate
  }
   def dateStrToMilliSeconds(a: String): Long = {
    val inputFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss")
    if (a == null || !a.contains("/")) {
      return 0
    }
    inputFormat.parse(a).getTime()
  }
  def dateStrToYearWeek(a: String): String = {
    val inputFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss")
    if (a == null || !a.contains("/")) {
      return null
    }
    val dateVal = inputFormat.parse(a)
    
    val weekyearStr = new SimpleDateFormat("w").format(dateVal)+'-'+new SimpleDateFormat("yyyy").format(dateVal)
    weekyearStr
  }
  def dateStrToWeekendDate(a: String): Date = {
    val inputFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss")
    if (a == null || !a.contains("/")) {
      return null
    }
    val dateVal = inputFormat.parse(a)
    val cal = Calendar.getInstance();
    val tmpSqlDate = new java.sql.Date(inputFormat.parse(a).getTime())
    //println(dateVal.getDay+":"+ dateVal.getMonth+":"+ dateVal.getYear)
    cal.setTime(tmpSqlDate)
    var sundayFound = true;     
    var sqlDate = tmpSqlDate
    do {
              val day = cal.get(Calendar.DAY_OF_WEEK);
              if (day == Calendar.SUNDAY) {
                  return new java.sql.Date(cal.getTimeInMillis);
                  sundayFound = true;
              }
              cal.add(Calendar.DAY_OF_YEAR, 1);
          }  while (sundayFound);
    sqlDate
  }
  
  
  def allSundaysOfAYear(year:Int):List[Date] = {
    
        val sundays = ListBuffer[Date]()
        val months:List[Int] = List(1,2,3,4,5,6,7,8,9,10,11,12)
        for(month <- months){
        val cal = new GregorianCalendar(year, month, 1);
          do {
              val day = cal.get(Calendar.DAY_OF_WEEK);
              if (day == Calendar.SUNDAY) {
                  sundays+=new java.sql.Date(cal.getTimeInMillis);
              }
              cal.add(Calendar.DAY_OF_YEAR, 1);
          }  while (cal.get(Calendar.MONTH) == month);
        }
        sundays.toList
  }
  
  def dateToMillSecond1(a: String): Long = {
    //val inputFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    val inputFormat = new SimpleDateFormat("yyyy-MM-dd")
    if (a == null || !a.contains("-")) {
      return 0
    }
    //if(a.contains("/")){
      val sdate = inputFormat.parse(a);
      return (sdate.getTime())
    //}
   //return 0
    
  }
  
  def dateToMillSecond2(a: String): Long = {
    val inputFormat = new SimpleDateFormat("MM/dd/yyyy")
    if (a == null) {
      return 0
    }
      val sdate = inputFormat.parse(a);
      return (sdate.getTime())
  }
  def removeCommaFormatter(input: String): String = {
    if (input == null) {
      return ""
    }
    return input.filterNot(x => x == ',')
  }
   def getWeekFromDate(input: String): String = {
    if(input == null || input =="" || input == " " ){
      return "0"
    }
    val dateFormat = new SimpleDateFormat("MM/dd/yyyy")
    val dateValue = dateFormat.parse(input)
    val cal = Calendar.getInstance()
    cal.setTime(dateValue)
    //cal.setFirstDayOfWeek(Calendar.THURSDAY)
    cal.setMinimalDaysInFirstWeek(1)
    val weekOfMonth = cal.get(Calendar.WEEK_OF_MONTH)
    val weekOfYear = cal.get(Calendar.WEEK_OF_YEAR) 
    
    return weekOfYear+"_"+cal.get(Calendar.YEAR)
  }
    def division(numerator: Long, denominator: Long): Double ={
    if(denominator == 0.0 )
      return 0
      val ans = numerator.toDouble / denominator.toDouble
    return ((ans * 100).round / 100.toDouble)
  }
    
   
	 
	  def getStartDayOfWeek(input: String): String = {
      if(input == null || input =="" || input == " " ){
      return "0"
    }
    val dateFormat = new SimpleDateFormat("MM/dd/yyyy")
    val dateValue = dateFormat.parse(input)
    val cal = Calendar.getInstance()
     cal.setTime( dateFormat.parse(input));
    val dayOfWeek = cal.get(Calendar.DAY_OF_WEEK)  

        
    val  r = DateTimeFormat.forPattern("MM/dd/YYYY").print(dateValue.getTime() - ((dayOfWeek-1) * (1000 * 60 * 60 * 24)))
    
    return r
  }
	  /*
     def getStartDayOfWeek(input: String): String = {
      if(input == null || input =="" || input == " " ){
      return "0"
    }
    val dateFormat = new SimpleDateFormat("MM/dd/yyyy")
    val dateValue = dateFormat.parse(input)
    val cal = Calendar.getInstance()
     
    val dayOfWeek = cal.get(Calendar.DAY_OF_WEEK)    
    val  r = DateTimeFormat.forPattern("MM/dd/YYYY").print(dateValue.getTime() - ((dayOfWeek) * (1000 * 60 * 60 * 24)))
    
    return r
  } */
	  
     def getEndDayOfWeek(input: String): String = {
     if(input == null || input =="" || input == " " ){
      return "0"
    }
    
    var dateFormat = new SimpleDateFormat("MM/dd/yyyy")
     if(input.length() > 13)
       dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss")
    val dateValue = dateFormat.parse(input)
    val cal = Calendar.getInstance()
    cal.setTime(dateValue)
    val dayOfWeek = 8 - cal.get(Calendar.DAY_OF_WEEK) 
    val  r = DateTimeFormat.forPattern("MM/dd/YYYY").print(dateValue.getTime() + ((dayOfWeek) * (1000 * 60 * 60 * 24)))
    return r
   }
     def convertDoubleToLong(input :Double):Int = (input).asInstanceOf[Int]
     def dateTimeToMillSecond(a: String): Long = {
    val inputFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss")
    if (a == null || !a.contains("/")) {
      return 0
    }
   inputFormat.parse(a).getTime()
  }

  def calculateGSATMetrics(createdateStr: String, sloDateStr: String, compeletedDateStr: String): String = {
//    var createdDateDateMS = UtilityClass.dateToMillSecond1(createdateStr.substring(0, 10))
//    var sloDateMS = UtilityClass.dateToMillSecond1(sloDateStr)
//    var compeletedDateMS = UtilityClass.dateToMillSecond1(compeletedDateStr)
    var createdDateDateMS = UtilityClass.dateToMillSecond2(createdateStr.substring(0, 10))
    var sloDateMS = UtilityClass.dateToMillSecond2(sloDateStr.substring(0, 10))
    var compeletedDateMS = UtilityClass.dateToMillSecond2(compeletedDateStr.substring(0, 10))
    var resopnseStr = "";
    if (sloDateMS > compeletedDateMS) {
      resopnseStr = "GSAT_BEFORE"
    } else if (sloDateMS < compeletedDateMS) {
      resopnseStr = "GSAT_AFTER"
    } else {
      resopnseStr = "GSAT_ON_TIME"
    }
    resopnseStr
  }
  def renameAndSemiComun(input: String*):String = {
    var result = ""
    input.map(f => result +=';'+f)
    result = result.substring(1)
    result
  }
  def timeDiffForGCT(str:String,str2:String):Long ={
    
    val inputFormat = new SimpleDateFormat("MM/dd/yyyy")
   
    val sdate = inputFormat.parse(str);
    val sdate2 = inputFormat.parse(str2);
    val diff = sdate2.getTime()-sdate.getTime()
    var retutn:Long = 0
    if(diff > 0 )
      retutn = diff / (1000 * 60 * 60 * 24)
    retutn  
    //DateTimeFormat.forPattern("MM/dd/YYYY").print(diff)
  }
   def substringFn(str: String) = {
    str.substring(0, 10)
  }
}