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
	def convert(record: (String, String, Double, Long, Long)): String = {
		val string = record._1+","+record._2+","+record._3+","+record._4+","+record._5
		return string
	}

	def getTypoRecords(sc: SparkContext, inFilePath: String, outFilePath: String) = {
		
		val webFilesDir = "./webfiles/"
		val pairRecords = sc.textFile(inFilePath).map(x => convert(x)).toArray

		//val outputFileStr = "./tmp_res"
		//val outFileWriter2 = new FileWriter(outputFileStr, true)
		//val outFileBufferWriter2 = new BufferedWriter(outFileWriter2)
		val hashMap = scala.collection.mutable.HashMap[String, String]()
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
	  				//outFileBufferWriter2.write(typo+",Yes\n")
	  				hashMap.+=((typo, "yes"))
	  			else
	  				//outFileBufferWriter2.write(typo+",No\n")
	  				hashMap.+=((typo, "no"))
			}
		}
		val outFileWriter2 = new FileWriter(inFilePath, false)
		val outFileBufferWriter2 = new BufferedWriter(outFileWriter2)
		for(record <- pairRecords){
			outFileBufferWriter2.write(convert(record)+","+hashMap(record._1)+"\n")
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

	def convertWithStatus(line: String): (String, String, Double, Long, Long, String) = {
		val record = org.apache.commons.lang.StringUtils.split(line, ",")	
		return (record(0), record(1), record(2).toDouble, record(3).toLong, record(4).toLong, record(5))
	}

	def getExistedAndNonRec(sc: SparkContext, inFilePath: String, outFilePath: String): Unit = {

		println(outFilePath)
		val webFilesDir = "./webfiles/"
		val pairRecords = sc.textFile(inFilePath).map(x => convertWithStatus(x)).toArray

		if(pairRecords.length == 0)
			return

		//Create output Dir
		val tmpArr = inFilePath.split("/")
		val outFileName = tmpArr.apply(tmpArr.length-1)

		val exsitedPath = "existed/"
		val nonextistedPath = "nonexisted/"
		println(outFilePath+exsitedPath)
		val efile = new File(outFilePath+exsitedPath)
		if(!efile.exists()){
			efile.mkdir
		}
		println(outFilePath+nonextistedPath)
		val nfile = new File(outFilePath+nonextistedPath)
		if(!nfile.exists()){
			nfile.mkdir
		}

		//File write buffer for existed domain
		println(outFilePath+exsitedPath+outFileName)
		println(outFilePath+nonextistedPath+outFileName)
		val existedOutFileWriter = new FileWriter(outFilePath+exsitedPath+outFileName, false)
		val existedOutFileBufferWriter = new BufferedWriter(existedOutFileWriter)

		//File write buffer for non-existed domain

		val nonOutFileWrite = new FileWriter(outFilePath+nonextistedPath+outFileName, false)
		val nonOutFileBufferWriter = new BufferedWriter(nonOutFileWrite)


		//Read Data File
		val nameWithDot = pairRecords.apply(0)._2
		val filenameBuffer = new scala.collection.mutable.StringBuilder()
		filenameBuffer.append(nameWithDot)
		val dataFile = new File(webFilesDir+filenameBuffer.dropRight(1).toString)

	
		if(dataFile.exists()){
			val dnsRecords = sc.textFile(webFilesDir+filenameBuffer.dropRight(1).toString).map(x => {new ParseDNSFast().convert(x)}).cache
			val popRecords = dnsRecords.filter(r => {
					val domain = new parseUtils().parseDomain(r._5, nameWithDot)
					domain == nameWithDot
				}).toArray
			for(record <- pairRecords){
				val typo = record._1
				val typoRecords = dnsRecords.filter(r => {
					val domain = new parseUtils().parseDomain(r._5, typo)
					domain == typo
				}).cache()
				for(popRec <- popRecords){
					val time = popRec._1
					val domain = nameWithDot
					val ip = popRec._3
					val res = typoRecords.filter(r => {
						val tmpDomain = new parseUtils().parseDomain(r._5, typo)
						val tmpTime = r._1
						val tmpIp = r._3
						val flag = compareIpAddr(tmpIp,ip)
						flag && tmpDomain == typo && time - tmpTime > 0 && time - tmpTime <= 60
					}).toArray

					for(r <- res){
						if(record._6 == "yes")
							existedOutFileBufferWriter.write(typo+","+r._1+","+domain+","+time+","+ip.split('.').apply(0)+"."+ip.split('.').apply(1)+".0.0\n")
						else
							nonOutFileBufferWriter.write(typo+","+r._1+","+domain+","+time+","+ip.split('.').apply(0)+"."+ip.split('.').apply(1)+".0.0\n")
					}
				}
			}
		}
		existedOutFileBufferWriter.close
		nonOutFileBufferWriter.close
	}

	def getExistedAndNonDistr(sc: SparkContext, inFileDir: String) = {
		val exsitedPath = "existed/*"
		val nonextistedPath = "nonexisted/*"

		val eData = sc.textFile(inFileDir + exsitedPath)
		val nData = sc.textFile(inFileDir + nonextistedPath)

		val eSum = eData.map(x => {
			val time1 = x.split(',').apply(1).toLong
			val time2 = x.split(',').apply(3).toLong
			time2 - time1
		}).reduce(_+_)

		val nSum = nData.map(x => {
			val time1 = x.split(',').apply(1).toLong
			val time2 = x.split(',').apply(3).toLong
			time2 - time1
		}).reduce(_+_)

		val eCount = eData.count
		val nCount = nData.count

		val filename = "distrResult.txt"
		val outFileWrite = new FileWriter(inFileDir+filename, false)
		val outFileBufferWriter = new BufferedWriter(outFileWrite)
		outFileBufferWriter.write("For existed domain, there are "+eCount+" records, average time is " + (eSum.toDouble/eCount.toDouble)+"\n")
		outFileBufferWriter.write("For non-existed domain, there are "+nCount+" records, average time is " +(nSum.toDouble/nCount.toDouble)+"\n")
		outFileBufferWriter.close()

		eData.map(x => {
			val time1 = x.split(',').apply(1).toLong
			val time2 = x.split(',').apply(3).toLong
			time2 - time1
		}).saveAsTextFile(inFileDir+"existedData.txt")

		nData.map(x => {
			val time1 = x.split(',').apply(1).toLong
			val time2 = x.split(',').apply(3).toLong
			time2 - time1
		}).saveAsTextFile(inFileDir+"nonExistedData.txt")
	}

	def not_main(args: Array[String]): Unit = {
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
	  	//getTypoRecords(sc, inFileDir+args.apply(0), outFileStringBuilder.toString)
	  	//getExistedAndNonRec(sc, inFileDir+args.apply(0), outFileStringBuilder.toString)
	  	getExistedAndNonDistr(sc, "./res/distrRecord/")
	}
}
				