package org.edmond.script 

import java.io.File
import java.io.FileWriter
import java.io.BufferedWriter

import spark.SparkContext
import spark.SparkContext._

import org.edmond.dnsproc_spark._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.edmond.utils._
import org.edmond.webs.test
import scala.util.control.Breaks._

object script_test extends Serializable {
	def count(sc: SparkContext, inFilePath: String, outFilePath: String): Unit = {
		val dnsLookups = sc.textFile(inFilePath, 20).map(x => {
			new ParseDNSFast().convert(x)
			})
		val count = dnsLookups.count

		val outFileWriter = new FileWriter(outFilePath, false)
		val outFileBufferWriter = new BufferedWriter(outFileWriter)
		outFileWriter.write(inFilePath+": "+count+"\n")
		outFileWriter.close

	}

	def compareIpAddr(ip_addr1:String, ip_addr2:String): Boolean = {
		//println(ip_addr1)
		//println(ip_addr2)
		val ipArr1 = ip_addr1.split('.')
		val ipArr2 = ip_addr2.split('.')
		if(ipArr1.length < 4 || ipArr2.length < 4)
			return false
		if(ipArr1.apply(0) == ipArr2.apply(0) && ipArr1.apply(1) == ipArr2.apply(1))
			return true
		else
			return false
	}


	def getSingleCount(popDomain: String, typoDomain: String, records: spark.RDD[(Int, Int, String, String, String, Int, Int, List[Array[String]], List[Array[String]], List[Array[String]])]): Long = {
		val set = records.filter(x => {
			val tmp = new parseUtils().parseDomain(x._5, popDomain)
			tmp == typoDomain
		})
		return set.count
	}

	def getJoinCount(popDomain: String, typoDomain: String, records: spark.RDD[(Int, Int, String, String, String, Int, Int, List[Array[String]], List[Array[String]], List[Array[String]])]): Long = {
		val typoArr = records.filter(x => {
			val tmp = new parseUtils().parseDomain(x._5, popDomain)
			tmp == typoDomain
		}).toArray

		val popRDD = records.filter(x =>{
			val tmp = new parseUtils().parseDomain(x._5, popDomain)
			tmp == popDomain
		})
		var count = 0.toLong
		if(typoArr.length == 0)
			return 0.toLong
		for(typo <- typoArr){
			val tmpCount = popRDD.filter(rec => {
				val duration = rec._1 - typo._1
				val flag = compareIpAddr(rec._3, typo._3)
				duration > 0 && duration < 60 && flag
			}).count
			count += tmpCount
		}

		return count
	}

	def isPopular(domain: String, popWebArr: Array[String]): Boolean = {
		val domainBuffer = new scala.collection.mutable.StringBuilder()
		domainBuffer.append(domain)
		if(!domain.endsWith("."))
			domainBuffer.append(".")
		val popWebBuffer = new scala.collection.mutable.StringBuilder()
		for(popWeb <- popWebArr){
			popWebBuffer.clear
			popWebBuffer.append(popWeb)
			if(popWeb.endsWith("."))
				popWebBuffer.append(".")
			if(popWebBuffer.toString == domainBuffer.toString)
				return true
		}
		return false
	}

	def getPairDistribution(sc: SparkContext, inFilePath: String, outFilePath: String): Unit = {
		/*
		val inFileDir = "./res/domainAndTypo/"
		val webFilesDir = "./webfiles/"
		val outPutDirPath = "./res/distribution/"
		val popWebFile = "./weblist/top1000"

		val outPutDir = new File(outPutDirPath)
		if(!outPutDir.exists()){
			outPutDir.mkdir
		}*/

		val popWebFile = "./weblist/top1000"
		val webFilesDir = "./webfiles/"

		val popWebArr = sc.textFile(popWebFile).toArray

		//val domainAndTypoFileList =  new ListFiles().recursiveListFiles(new File(inFileDir))
		val webFilesList = new ListFiles().recursiveListFiles(new File(webFilesDir))

		//for(domainFile <- domainAndTypoFileList){

		//val domainAndTypo = sc.textFile(domainFile.toString).toArray
		val domainAndTypo = sc.textFile(inFilePath).toArray
		val outFileWriter = new FileWriter(outFilePath, false)
		val outFileBufferWriter = new BufferedWriter(outFileWriter)
		for(line <- domainAndTypo){
			val domainsArr = line.split(" ")
			if(domainsArr.length>1){
				val popDomain = domainsArr.apply(0)
				//Check whether the file named as popDomain exists
				var flag = false
				var tmpName = ""
				for(fileTmp <- webFilesList){
					breakable{
						val name = fileTmp.getName
						if((name+".") == popDomain){
							flag = true
							tmpName = name
							break
						}
					}
				}
				if(flag){
					val dnsRecords = sc.textFile(webFilesDir+tmpName, 20).map(x => {
						new ParseDNSFast().convert(x)
					})
						for(typoCandidate <- domainsArr){
							if(typoCandidate != popDomain){
								//a typo cannot be a popular websites
								val whetherPop = isPopular(typoCandidate, popWebArr)
								if(!whetherPop){
									val countSingle = getSingleCount(popDomain, typoCandidate, dnsRecords)
									val countJoin = getJoinCount(popDomain, typoCandidate, dnsRecords)

									if(countSingle != 0 && (countJoin.toFloat/countSingle.toFloat) > 0.1){
										outFileBufferWriter.write(typoCandidate+","+popDomain+","+(countJoin.toFloat/countSingle.toFloat)+","+countJoin+","+countSingle+"\n")
									}
								}
							}
						}
					}
				}
		}
		outFileBufferWriter.close

		//}
	}

	def convert(line: String): (String, String, Double, Long, Long) = {
		val record = org.apache.commons.lang.StringUtils.split(line, ",")	
		return (record(0), record(1), record(2).toDouble, record(3).toLong, record(4).toLong)
	}

	def getTypoRecords(sc: SparkContext, inFilePath: String, outFilePath: String) = {
		
		val webFilesDir = "./webfiles/"
		val pairRecords = sc.textFile(inFilePath).map(x => convert(x)).toArray

		val outputFileStr = "./tmp_res"
		val outFileWriter2 = new FileWriter(outputFileStr, true)
		val outFileBufferWriter2 = new BufferedWriter(outFileWriter2)
		for(record <- pairRecords){
			val typo = record._1
			val popDomain = record._2
			val filenameBuilder = new scala.collection.mutable.StringBuilder()
			filenameBuilder.append(popDomain)
			val filename = filenameBuilder.splitAt(filenameBuilder.length-1)._1

			//check whether the file is existed in ./webfiles
			val recordFile = new File(webFilesDir+filename.toString)
			if(recordFile.exists){
				val dnsRecords = sc.textFile(webFilesDir+filename.toString, 20).map(x => {
					new ParseDNSFast().convert(x)
				})
				val typoRecords = dnsRecords.filter(record => {
					val tmp = new parseUtils().parseDomain(record._5, popDomain)
					tmp == typo
				})
				val outFileWriter = new FileWriter(outFilePath+typo, false)
				val outFileBufferWriter = new BufferedWriter(outFileWriter)
				typoRecords.map(record => new ParseDNSFast().antiConvert(record)).toArray.foreach(rcd => {
	  				outFileBufferWriter.write(rcd+"\n")
	  			})
	  			outFileBufferWriter.close
	  			val bool = isExistedWebsite(sc, outFilePath+typo)
	  			if(bool)
	  				outFileBufferWriter2.write(typo+",Yes\n")
	  			else
	  				outFileBufferWriter2.write(typo+",No\n")
			}
		}
		outFileBufferWriter2.close
	}

	def isRootNameServer(ns: String): Boolean ={
		val rootNS = Array("A.GTLD-SERVERS.NET.",
						"G.GTLD-SERVERS.NET.",
						"H.GTLD-SERVERS.NET.",
 						"C.GTLD-SERVERS.NET.",
 						"I.GTLD-SERVERS.NET.",
 						"B.GTLD-SERVERS.NET.",
 						"D.GTLD-SERVERS.NET.",
 						"L.GTLD-SERVERS.NET.",
 						"F.GTLD-SERVERS.NET.",
 						"J.GTLD-SERVERS.NET.",
 						"K.GTLD-SERVERS.NET.",
 						"E.GTLD-SERVERS.NET.",
 						"M.GTLD-SERVERS.NET.")
		for(root <- rootNS){
			if(root.equalsIgnoreCase(ns))
				return true
		}
		return false
	}


	def isExistedWebsite(sc: SparkContext, inFilePath: String): Boolean = {
		val dnsRecords = sc.textFile(inFilePath, 20)map(x => {new ParseDNSFast().convert(x)})
		val totalCount = dnsRecords.count
		var recordsArr= Array[(Int, Int, String, String, String, Int, Int, List[Array[String]], List[Array[String]], List[Array[String]])]()
		if(totalCount > 20){
			recordsArr = dnsRecords.take(20)
		}
		else{
			recordsArr = dnsRecords.toArray
		}
		object Yes extends Exception { }
		object No extends Exception { }
		val tmpArr = inFilePath.split("/")
		val filename = tmpArr.apply(tmpArr.length-1) 
		var root_count = 0
		var server_count = 0
		for(record <- recordsArr){
		  	val ans_sec = record._8
		  	val auth_sec = record._9
		  	val add_sec = record._10
		  	if(ans_sec.length == 0 && auth_sec.length == 0 && add_sec.length == 0){
		  		//nothing in this record
		  		return false
		  	}

		  	for(answer <- ans_sec){
		  		if(answer.apply(0) == filename && answer.apply(3)=="A"){
		  			//If there is a A rocord in answer section, it exists absolutely
		  			return true
		  		}
		  		if(answer.apply(3) == "MX"){
		  			if(!isRootNameServer(answer.apply(4).split(" ").apply(1)))
		  				return true
		  		}
		  	}
		  	var flag = true
		  	for(auth <- auth_sec){
		  		if(auth.apply(3) == "NS"){
		  			if(flag){
		  				if(!isRootNameServer(auth.apply(4).split(" ").apply(0)))
		  				//if there is one NS record for non-root NS
		  					server_count+=1
		  				else
		  					root_count+=1
		  				flag = false
		  			}
		  		}
		  		else if(auth.apply(3) == "SOA"){
		  			if(isRootNameServer(auth.apply(4).split(" ").apply(0)))
		  				return false
		  		}
		  		else if(auth.apply(3) == "MX"){
		  			if(!isRootNameServer(auth.apply(4).split(" ").apply(1)))
		  				return true
		  		}
		  	}
		  	//if(root_count == auth_sec.length)
		  	// if all NS records in auth are about root NS
		  	//	return false

		}
		if(server_count < root_count)
		  	return false
		return true
	}

	def main(args: Array[String]): Unit = {
		println("This is a script-started job")

		if(args.length < 2){
			println("uage: ./sbt run inFilePath outFileDir")
			exit(0)
		}

		System.setProperty("spark.default.parallelism","500")
	  	Logger.getLogger("spark").setLevel(Level.INFO)

	  	val sparkHome = "/Users/edmond/spark-0.7.3"
	  	val jarFile = "target/scala-2.9.3/dnsudf_spark_2.9.3-0.0.jar"
	  	val master = "local[20]"
		val sc = new SparkContext(master, "dnsudf_spark", sparkHome, Seq(jarFile))
	  	//val inFileDir = "./res/domainAndTypo/" //For getDistribution()
	  	val inFileDir = "./res/distribution/" //For getTypoRecords()
	  	val outFileDir = "./res/"


		val outFileStringBuilder = new scala.collection.mutable.StringBuilder()
		outFileStringBuilder.append(outFileDir)
		outFileStringBuilder.append(args.apply(1))
		if(!outFileStringBuilder.toString.endsWith("/"))
			outFileStringBuilder.append("/")
		val outFile = new File(outFileStringBuilder.toString)
		if(!outFile.exists())
			outFile.mkdir()
	  	//getPairDistribution(sc, inFileDir+args.apply(0), outFileStringBuilder.toString+arg.apply(0))
	  	getTypoRecords(sc, inFileDir+args.apply(0), outFileStringBuilder.toString)
	}
}
				