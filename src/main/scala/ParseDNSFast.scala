package org.edmond.dnsproc_spark

import spark.SparkContext
import spark.SparkContext._
import spark.util.Vector

import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.util.control.Breaks._
import scala.util.Random
import scala.io.Source
import scala.io._
import java.io.File


/*
object PigConversions{
	def listToBag(t: List[Array[String]], sc: SparkContext) : spark.RDD[Array[String]] = {
		val arr_arr : Array[List[String]] = new Array[List[String]](t.length)
		for(i <- 0 until t.length){
			arr_arr.update(i, t(i).toList)
		}
		return sc.parallelize(arr_arr.toSeq)
	}

	implicit def arrayToList(arr: Array[String]) : List = {
		return arr.toList	
	}

	def arrayToTuple(t: Array[String], sc: SparkContext) : spark.RDD[String] ={
		return sc.parallelize(t)
	}

	def listtoTuple(t: List[String], sc: SparkContext) : spark.RDD[String] = {
		return sc.parallelize(t)
	}
}

*/

class DNSInfo(
	//0: ts
	val ts : Int,
	//1: type
	val typ : Int,
	//2: src_ip
	val src_ip : String,
	//3: dst_ip
	val dst_ip : String,
	//4: domain
	val domain : String,
	//5: query type
	val qtype : Int,
	//6: rcode
	val rcode : Int,
	//Everything else
	val remaining :Array[String])
{
	val parsed_answer : (List[Array[String]], List[Array[String]], List[Array[String]]) = parseAnswer()


	def toTuple() : (Int, Int, String, String, String, Int, Int, List[Array[String]],List[Array[String]],List[Array[String]]) = {
		val out = (ts,
			typ,
			src_ip,
			dst_ip,
			domain.toLowerCase,
			qtype,
			rcode,
			parsed_answer._1,
			parsed_answer._2, 
			parsed_answer._3)
		/*
			listToBag(parsed_answer._1)
			listToBag(parsed_answer._2)
			listtoBag(parsed_answer._3))
		*/
		return out		
	}

	def parseAnswerPart(rows: Int, start_pos: Int, row: Array[String]) :(Int, List[Array[String]]) = {
		var pos = start_pos
		var answer = scala.collection.mutable.ListBuffer[Array[String]]()

		for(x <- 0.until(rows)){
			val cols = remaining(pos).toInt
			pos += 1
			if(cols != 0){
				answer += {
					val tmp = remaining.slice(pos, pos+cols)
					tmp.slice(0,4) :+ tmp.slice(4, tmp.length).mkString(";")
				}
				pos += cols
			}
		}
		//println("Parsed: " + answer)
		return (pos, answer.toList)
	}

	def parseAnswer() : (List[Array[String]], List[Array[String]], List[Array[String]]) = {
		try {

		  //Parse the answer section
		  val rows = remaining(0).toInt
		  var pos = 1

		  val answer = if(rows > 0){
		  	val ret = parseAnswerPart(rows, pos, remaining)
		  	pos = ret._1
		  	ret._2
		  } else { Nil }

		  //parse the authoritative section
		  val arows = remaining(pos).toInt
		  pos += 1

		  val authoritative = if(arows > 0) {
		  	val ret = parseAnswerPart(arows, pos, remaining)
		  	pos = ret._1
		  	ret._2
		  } else {Nil}

		  // parse the additional section
		  val brows = remaining(pos).toInt
		  pos += 1

		  val additional = if(brows > 0) {
		  	val ret = parseAnswerPart(brows, pos, remaining)
		  	pos = ret._1
		  	ret._2
		  } else {Nil}

		  answer.foreach(line => { if(line.size != 5) System.out.println("odd sized (%d) answer line: (%s)\n(%s)".format(line.size, line.toList, remaining.toList))})

		  return (answer, authoritative, additional)
		} catch {
		  case e: Exception => {
		  	//println("failed to parse answer section: " + e + remaining.toList.mkString(","))
		  	return (Nil, Nil, Nil)
		  }
		}
	}

	override def toString() : String = "ts=%d, type=%d, src_ip=%s, dst_ip=%s, domain=%s, qtype=%d, rcode=%d".format(ts, typ, src_ip, dst_ip, domain, qtype, rcode)
}

object ParseDNSFast {
	def fasterSplit(line: String) : Array[String] = {
		var pos = 0
		var result = new collection.mutable.Queue[String]()

		while(pos <= line.lastIndexOf(",")){
			val next = line.indexOf(",", pos)
			result += line.substring(pos, next)
			pos = next + 1
		}

		result += line.substring(pos, line.length)
		return result.toArray
	}
}

class ParseDNSFast {
/*	def exec(input: Tuple): (Int, Int, String, String, String, Int, Int, Array[String]) = {
		val line = input._1.asInstanceOf[String]
		try { 
		  return convert(line)
		} catch {
		  case e: Exception => {
		  	println("falied to parse: " + e + line)
		  	return null
		  }
		}
	}*/

	def print() = {
		println("This is from ParseDNSFast Class")
	}

	def convert(line: String) : (Int, Int, String, String, String, Int, Int, List[Array[String]], List[Array[String]], List[Array[String]]) = {
		//println(line)
		val parts = org.apache.commons.lang.StringUtils.split(line, ",")
		try {
			if(parts(4).startsWith("\"")){
		  		var tmp = 4
		  		breakable{
		  			while(tmp < parts.length){
		  				if(parts(tmp).endsWith("\"") && !parts(tmp).endsWith("\"\"") && !(tmp == 4 && parts(tmp).length==1))
		  					break
		  				else
		  				tmp += 1
                        }
                    }

                    var tmp2 = tmp - 4
                    var str = parts(4)
                    for( i <- 0 until tmp2){
                    	str += "," + parts(4+i+1)
                    }
                    return (new DNSInfo(parts(0).toInt, parts(1).toInt, parts(2), parts(3), str,
                    	parts(tmp+1).toInt, parts(tmp+2).toInt, parts.slice(tmp+3, parts.length))).toTuple
            }
            else{
            	return (new DNSInfo(parts(0).toInt, parts(1).toInt, parts(2), parts(3), parts(4), parts(5).toInt, parts(6).toInt, parts.slice(7, parts.length))).toTuple
            }
		  //return (new DNSInfo(parts(0).toInt, parts(1).toInt, parts(2), parts(3), parts(4), parts(5).toInt, parts(6).toInt, parts.slice(7, parts.length))).toTuple
		} catch {
			// right now the only exception we can handle is if the domain is 
			// formatted incorrectly
		  case _ : java.lang.NumberFormatException =>
		  	println("Possible error while parsing " + line)

		  	return (new DNSInfo(parts(0).toInt, parts(1).toInt, parts(2), parts(3), parts(4)+parts(5), 
		  			parts(6).toInt, parts(7).toInt, parts.slice(8, parts.length))).toTuple
		}
	}

	def antiConvert(record: (Int, Int, String, String, String, Int, Int, List[Array[String]], List[Array[String]], List[Array[String]])) : String = {
		val line = new scala.collection.mutable.StringBuilder()
		line.clear
		line.append(record._1 + "," + record._2 + "," + record._3 + "," + record._4 + "," + record._5 + "," + record._6 + "," + record._7 + ",")
		line.append(record._8.size + ",")
		if(record._8.size != 0){
			//line.append(record._8.apply(0).size + ",")
			record._8.foreach(arr => {
				line.append(arr.size + ",")
				val str = arr.mkString(",")
				line.append(str + ",")
				})
		}
		line.append(record._9.size + ",")
		if(record._9.size != 0){
			//line.append(record._9.apply(0).size + ",")
			record._9.foreach(arr => {
				line.append(arr.size + ",")
				val str = arr.mkString(",")
				line.append(str + ",")
				})
		}
		line.append(record._10.size)
		if(record._10.size != 0){
			line.append(",")
			for(i <- 0 until record._10.size){
				line.append(record._10.apply(i).size + ",")
				val str = record._10.apply(i).mkString(",")
				line.append(str)


				if(i < (record._10.size - 1))
					line.append(",")
			}
		}
		return line.toString
	}	
}


object FastTest {

   // [1306627003, 0, '208.67.219.13', '184.82.51.169', 'www3.greatmarchblog.net', 1, 5, [], [], []]
   val test_values = "1306627003,0,208.67.219.13,184.82.51.169,www3.greatmarchblog.net.,1,5,0,0,0"::"1306627009,0,208.67.219.13,184.82.51.170,www3.greatmarchblog.net.,1,5,0,0,0"::
   "1306627010,0,208.67.219.12,184.82.51.168,www3.greatmarchblog.net.,1,5,0,0,0"::
   "1306627012,0,208.67.219.12,64.120.146.2,ns2.exserver.ru.,28,0,0,1,11,exserver.ru.,14400,IN,SOA,ns1.exserver.ru.,hostmaster.exserver.ru.,2011051401,14400,3600,1209600,86400,0"::
   "1306627015,0,208.67.219.13,124.246.68.118,wa.creative.com.,1,0,1,5,wa.creative.com.,3600,IN,A,216.137.18.78,0,0"::
   "1306627016,0,208.67.219.12,175.176.164.105,ns1.stmik-abg.ac.id.,28,2,0,0,0"::
   "1306627016,0,208.67.219.12,175.176.164.105,www.ika.stmik-abg.ac.id.,1,2,0,0,0"::
   "1306627016,0,208.67.219.12,184.82.51.171,www3.greatmarchblog.net.,1,5,0,0,0"::
   "1306627016,0,208.67.219.12,64.120.133.212,semenovichanna-info.ru.,15,5,0,0,0"::
   "1306627223,0,68.1.208.17,91.195.240.162,INODE=16,0xf0202840.parmacare.com.,1,2,0,0,0"::
   "1306627239,0,68.1.208.16,209.239.114.107,#\032This\032file\032is\032generated\032automatically\032by\032kernel(ADM),.parmacare.com.,1,2,0,0,0"::
   "1306627016,0,208.67.219.14,124.246.69.209,wa.creative.com.,1,0,1,5,wa.creative.com.,3600,IN,A,216.137.18.78,0,0"::Nil


   def get_count(line: (Int, Int, String, String, String, Int, Int, List[Array[String]], List[Array[String]], List[Array[String]]), data: spark.RDD[(Int, Int, String, String, String, Int, Int, List[Array[String]], List[Array[String]], List[Array[String]])]): Long = {
   	val domain = line._5.replace(".ocm.",".com.")
   	val result = data.filter(r => (r._5.equalsIgnoreCase(domain))).filter(r => !r._8.isEmpty).filter(r => ((r._8.apply(0).apply(1).toInt + r._1)< line._1)).count
   	return result
   }

   def recursiveListFiles(f: File): Array[File] = {
   	val these = f.listFiles
   	these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
   }

   def printRecord(record: (Int, Int, String, String, String, Int, Int, List[Array[String]], List[Array[String]], List[Array[String]])) = {
      print(record._1 + ", " + record._2 + ", " + record._3 + ", " + record._4 + ", " + record._5 + ", " + record._6 + ", " + record._7 + ", ")
      print("{")
      for (arr <- record._8){
         print("||")
         for(a <- arr){
            print("(" + a + ")")
         }
      }
      print("}, {")
      for (arr <- record._9){
         print("||")
         for(a <- arr){
            print("(" + a + ")")
         }
      }
      print("}, {")
      for (arr <- record._10){
         print("||")
         for(a <- arr){
            print("(" + a + ")")
         }
      }
      print("}\n")
   }

   def not_main(args: Array[String]): Unit = {       
   		//for(t <- test_values) {
   		//  println(ParseDNSFast.fasterSplit(t).mkString(", "))
   		//}
   		val outPath = "./"
   		Logger.getLogger("spark").setLevel(Level.WARN)
   		val sparkHome = "/Users/edmond/spark-0.7.3"
   		val jarFile = "target/scala-2.9.3/dnsudf_spark_2.9.3-0.0.jar"
   		val master = "local[1]"
   		val sc = new SparkContext(master, "dnsudf_spark", sparkHome, Seq(jarFile))
   		

   		
   		//val original_data = sc.parallelize(test_values)
   		//val original_data = sc.textFile("./spcRecord")
   		//val original_data = sc.textFile("/data1/sie/ch202/201212/*.0.gz")
   		val original_data = sc.textFile("/data1/sie/ch202/201212/test_data")
   		val out = new java.io.FileWriter(outPath + "ocm_result.txt")

   		try { 
   			val data = original_data.map( x => new ParseDNSFast().convert(x))

   			data.foreach(r => printRecord(r))

   			println("********************")

   			val data_2 = data.map(r => {
   				new ParseDNSFast().antiConvert(r)
   				})

   			data_2.foreach(println)

   			println("*******************")
   			data_2.map( x => new ParseDNSFast().convert(x)).foreach(printRecord)
   		//	val count = data.count()
   		//	println("Count is " + count)

   		
   		//	val sortedRecords = ocmRecords.map(r => (r._1, r)).sortByKey(true).map(r => r._2)

   			//sortedRecords.foreach(println)
   			
   		//	val number = sortedRecords.count

 
   		//	out.write("There are " + count + " records totally.\n")
   		//	out.write("The count of \"ocm.\" is " + number + ".\n")
   			//println(result.count)
   			//ocmRecords.saveAsTextFile("result.txt")
/*
   			sortedRecords.foreach(println)

   			val test_data = sortedRecords.mapPartitions(i => {
   				val j = i.buffered
   				j.flatMap(x => {
   					if (j.hasNext) List((x, j.head));
   					else List()
   					})
   				})
   			println("Count of ocm is " + number)
   			println(test_data.count)
   			test_data.foreach(r => println(r._1 + ", " + r._2))

   			val res = test_data.filter(r => (r._2._1 - r._1._1) < 300)

   			println(res.count)
   			println(sortedRecords.getClass)*/

		//	out.write("There are " + count + "records\n")
			//out.write("There are " + countOcmRcd + " records ending with \".ocm.\"\n")
		//	out.close
   		} catch {
     	case _ :  java.lang.NumberFormatException =>
   		}


	}
}