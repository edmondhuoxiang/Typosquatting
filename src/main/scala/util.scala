package org.edmond.utils

import java.util.Date
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.io.File
import scala.util.matching.Regex

import org.edmond.DLDistance._

class parseUtils{
	def parseDomain(domain_1: String, domain_2: String): String = {
		val num = domain_2.count(_ == '.') + 1
		val num_1 = domain_1.count(_ == '.')
		if(num_1 == num){
			return domain_1
		}
		val parts = domain_1.split('.').zipWithIndex
		val len = parts.length
		val res = parts.filter(r => r._2 > (len - num - 1)).map(r => r._1)
		return res.mkString(".")+"."
	}

	def convertStampToDate(timestamp: Int): java.util.Date = {
		//Convert timestamp to Date, the paramete timestamp is in second rather than  millisecond
		val stamp = new java.sql.Timestamp(timestamp.toLong * 1000) //convert timestamp to Milliseconds
		val date = new java.util.Date(stamp.getTime())
		return date
	}

	def convertDateToString(date: java.util.Date): String = {
		//Convert Date to String which matches part of file's name
		//The string representation of data
		val df = new SimpleDateFormat("yyyyMMdd.HH")

		val df_mm = new SimpleDateFormat("mm")

		val tmpStr = df_mm.format(date).toArray
		var num = (tmpStr(0) - '0') * 10 + (tmpStr(1) - '0')
		var num_2 = num % 10		
		if(num_2 >= 5)
			num = num - (num_2-5)
		else
			num = num - num_2
		// Using DateFormat format method to create a string
		// representation of a date with the defined format.
		var report = df.format(date)
		if (num < 10)
			report += 0.toString + num.toString
		else
			report += num.toString
		return report
	}

	def convertStampToFilename(timestamp: Int): Array[String] ={
		// Convert timestamp in second to filename
		//val initial_timestamp = 1354320000 //local machine
		//val end_timestamp = 1356916500 //local machine
		val initial_timestamp = 1354323600 //server side
		val end_timestamp = 1354324800 //server side
		val interval = 300
		var mid_timestamp = 0
		val err = 21600
		if(timestamp < initial_timestamp)
			mid_timestamp = initial_timestamp
		else if(timestamp > end_timestamp)
			mid_timestamp = end_timestamp
		else{
			val dist = (timestamp - initial_timestamp) % 300
			if(dist < 150){
				mid_timestamp = timestamp - dist
			}else
				mid_timestamp = timestamp + 300 - dist
		}
		if(mid_timestamp > initial_timestamp && mid_timestamp < end_timestamp){
			val date1 = convertStampToDate(mid_timestamp - interval + err)
			val date_string1 = convertDateToString(date1)
			val filename1 = "raw_processed." + date_string1 + ".*.0.gz"

			val date2 = convertStampToDate(mid_timestamp + err)
			val date_string2 = convertDateToString(date2)
			val filename2 = "raw_processed." + date_string2 + ".*.0.gz"

			val date3 = convertStampToDate(mid_timestamp + interval + err)
			val date_string3 = convertDateToString(date3)
			val filename3 = "raw_processed." + date_string3 + ".*.0.gz"

			return Array(filename1, filename2, filename3)
		}
		else
		{
			if(mid_timestamp == initial_timestamp){
				val date2 = convertStampToDate(mid_timestamp + err)
				val date_string2 = convertDateToString(date2)
				val filename2 = "raw_processed." + date_string2 + ".*.0.gz"

				val date3 = convertStampToDate(mid_timestamp + interval + err)
				val date_string3 = convertDateToString(date3)
				val filename3 = "raw_processed." + date_string3 + ".*.0.gz"

				return Array(filename2, filename3)
			}
			else{
				val date1 = convertStampToDate(mid_timestamp - interval + err)
				val date_string1 = convertDateToString(date1)
				val filename1 = "raw_processed." + date_string1 + ".*.0.gz"

				val date2 = convertStampToDate(mid_timestamp + err)
				val date_string2 = convertDateToString(date2)
				val filename2 = "raw_processed." + date_string2 + ".*.0.gz"

				return Array(filename1, filename2)
			}
		} 
	}

	def lookUpString(target: String, sortedArr: Array[String], start: Int, end: Int): Int = {
	/*	println("target: "+target)
		println("LENGTH: " + sortedArr.length)
		println("start: "+start)
		println("end: "+end)
	*/	if(start == end){
			val tmp = parseDomain(target, sortedArr.apply(start)+".")
			val distance = new DLDistance().distance(tmp, sortedArr.apply(start)+".")
			if( distance <=2 ){
				return start
			} else {
				return -1
			}
		}
		else if( start > end){
			return -1
		}

		var curIndex = -1
		curIndex = (start + end) / 2;
		//println("curIndex: "+curIndex)
		var middleValue = sortedArr.apply(curIndex)
		//println("middleValue: " + middleValue)
		val tmp_target = parseDomain(target, middleValue + ".")
		val dist = new DLDistance().distance(tmp_target, middleValue)
		if (dist <= 2){
			return curIndex
		}
		else if(target < middleValue){

			return lookUpString(target, sortedArr, start, curIndex-1)
		}
		else{
			return lookUpString(target, sortedArr, curIndex+1, end)
		}
	}	

}

class ListFiles(){
	def recursiveListFiles(f: File): Array[File] = {
		val these = f.listFiles
		these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles(_))
	}

	def recursiveListFiles(f: File, r: Regex): Array[File] = {
		val these = f.listFiles
		val good = these.filter(f=>r.findFirstIn(f.getName).isDefined)
		good ++ these.filter(_.isDirectory).flatMap(recursiveListFiles(_,r))
	}
	
}