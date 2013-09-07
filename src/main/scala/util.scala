package org.edmond.utils

import java.util.Data
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

	def convertDateToString(date: Date): String = {
		//Convert Date to String which matches part of file's name
		//The string representation of data
		val df = new SimpleDateFormat("yyyyMMdd.HH")

		val df_mm = new SimpleDateFormat("mm")

		val tmpStr = df_mm.format(date).toArray
		val num = (tmpStr(0) - '0') * 10 + (tmpStr(1) - '0')
		val num_2 = num % 10		
		if(num_2 >= 5){}
			num = num - (num_2-5)
		else
			num = num - num_2
		// Using DateFormat format method to create a string
		// representation of a date with the defined format.
		String report = df.format(date) + num.toString
		return report
	}


}