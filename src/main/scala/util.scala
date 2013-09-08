package org.edmond.utils

import java.util.Date
import java.sql.Timestamp
import java.text.SimpleDateFormat

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
		val report = df.format(date) + num.toString
		return report
	}

	def convertStampToFilename(timestamp: Int): Array[String] ={
		// Convert timestamp in second to filename
		val initial_timestamp = 1354320000
		val interval = 300
		var mid_timestamp = 0
		val err = 21600
		if(timestamp < initial_timestamp)
			mid_timestamp = initial_timestamp
		else{
			val dist = (timestamp - initial_timestamp) % 300
			if(dist < 150){
				mid_timestamp = timestamp - dist
			}else
				mid_timestamp = timestamp + 300 - dist
		}
		if(mid_timestamp > initial_timestamp){
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
		else{
			val date2 = convertStampToDate(mid_timestamp + err)
			val date_string2 = convertDateToString(date2)
			val filename2 = "raw_processed." + date_string2 + ".*.0.gz"

			val date3 = convertStampToDate(mid_timestamp + interval + err)
			val date_string3 = convertDateToString(date3)
			val filename3 = "raw_processed." + date_string3 + ".*.0.gz"

			return Array(filename2, filename3)
		}
	}
}