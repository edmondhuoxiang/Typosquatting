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

	def getDistribution(sc: SparkContext, inFilePath: String, outFilePath: String): Unit = {
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

	def main(arg: Array[String]) = {
		println("This is a script-started job")

		if(arg.length < 2){
			println("uage: ./sbt run inFilePath outFileDir")
			exit(0)
		}

		System.setProperty("spark.default.parallelism","500")
	  	Logger.getLogger("spark").setLevel(Level.INFO)

	  	val sparkHome = "/Users/edmond/spark-0.7.3"
	  	val jarFile = "target/scala-2.9.3/dnsudf_spark_2.9.3-0.0.jar"
	  	val master = "local[20]"
		val sc = new SparkContext(master, "dnsudf_spark", sparkHome, Seq(jarFile))
	  	val inFileDir = "./res/domainAndTypo/"
	  	val outFileDir = "./res/"


		val outFileStringBuilder = new scala.collection.mutable.StringBuilder()
		outFileStringBuilder.append(outFileDir)
		outFileStringBuilder.append(arg.apply(1))
		if(!outFileStringBuilder.toString.endsWith("/"))
			outFileStringBuilder.append("/")
		val outFile = new File(outFileStringBuilder.toString)
		if(!outFile.exists())
			outFile.mkdir()
	  	getDistribution(sc, inFileDir+arg.apply(0), outFileStringBuilder.toString+arg.apply(0))
	}
}
				