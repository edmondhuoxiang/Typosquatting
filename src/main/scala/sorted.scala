package org.edmond.sorted 

import java.io.File
import java.io.FileWriter
import java.io.BufferedWriter
import java.io._

import spark.SparkContext
import spark.SparkContext._

import org.edmond.dnsproc_spark._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.edmond.utils._
import org.edmond.webs.test
import scala.util.control.Breaks._

import org.edmond.utils._
import org.edmond.DLDistance._
import scala.util.matching.Regex
import sys.process._

object sorted extends Serializable {
	def sortedDataViaTime(sc: SparkContext, inFilePath: String, outFilePath: String) = {

		println(inFilePath)
		println(outFilePath)
		val data = sc.textFile(inFilePath, 20)

		val dnsRecords = data.map(x => {
			new ParseDNSFast().convert(x)
		}).map(x => (x._1, x))

		val sortedRecords = dnsRecords.sortByKey().map(x => x._2)

		sortedRecords.map(x => new ParseDNSFast().antiConvert(x)).saveAsTextFile(outFilePath)
	}

	def convertFilename(str: String): String = {
		val tmp = str.split('/')
		val filename = tmp.apply(tmp.length-1)

		val filenameArr = filename.split('.')
		val outFilename = new scala.collection.mutable.StringBuilder()
		var index = 0
		for(string <- filenameArr){
			if(index != filenameArr.length-1){
				outFilename.append(filenameArr.apply(index).toUpperCase)
				if(index != filenameArr.length-2)
					outFilename.append(".")
			}
			index+=1
		}

		return outFilename.toString
	}

	def preprocessing(sc: SparkContext, inFilePath: String): Unit = {
		println(inFilePath)
		val data = sc.textFile(inFilePath).map(line => line.split(" ")).toArray
		if (data.length <= 1){
			if(data.length == 0){
				val file = new File(inFilePath)
				file.delete
			}
			return
		}
		val tmpArr = inFilePath.split('/')
		val dirStringBuilder = new scala.collection.mutable.StringBuilder()
		var index=0
		for(string <- tmpArr){
			if(index != tmpArr.length-1){
				dirStringBuilder.append(tmpArr.apply(index))
				dirStringBuilder.append("/")
			}
			index+=1
		}

		for(record <- data){
			val filename = record.apply(0)
			println("filename = "+filename)
			val outFileWrite = new FileWriter(dirStringBuilder.toString+convertFilename(filename), false)
			val outFileBufferWriter = new BufferedWriter(outFileWrite)
			outFileBufferWriter.write(record.mkString(" "))
			outFileBufferWriter.close
		}
	}

	def preprocessingAll(sc: SparkContext, inFileDir: String): Unit = {
		val dir = new File(inFileDir)
		if(dir.exists()){
			val filesArr = new ListFiles().recursiveListFiles(dir)
			for(file <- filesArr){
				preprocessing(sc, file.getAbsolutePath)
			}
		}
	}

	def cleanAndDivide(sc: SparkContext): Unit = {
		val inFileDir="./res/domainAndTypo/"
		val outFileDir = "./res/domainAndTypo/"
		val webFiles = "/Users/edmond/Typosquatting/weblist/500_1000"

		val popWebsites = sc.textFile(webFiles).toArray

		//get list of all files
		val fileList = new ListFiles().recursiveListFiles(new File(inFileDir))
		for(file <- fileList){
			println(file.toString)
			val records = sc.textFile(file.toString).toArray
			if(records.length > 0){
				val line = records.apply(0)
				val domainArr = line.split(" ")
				val popDomain = new scala.collection.mutable.ArrayBuffer[String]()
				for(domain <- domainArr){
					val domainBuffer = new scala.collection.mutable.StringBuilder()
					domainBuffer.append(domain)
					if(!domain.endsWith("."))
						domainBuffer.append(".")
					for(popWeb <- popWebsites){
						//println("popWebsites: "+popWeb)
						val popWebBuffer = new scala.collection.mutable.StringBuilder()
						popWebBuffer.append(popWeb)
						if(!popWeb.endsWith("."))
							popWebBuffer.append(".")
						breakable{
							//println("domain:" + domainBuffer.toString+", popWeb: "+ popWebBuffer.toString)
							if(domainBuffer.toString == popWebBuffer.toString){

								popDomain.+=(domainBuffer.toString)
								break
							}
						}
					}	
				}

				//Add all pop domains in a HashMap as keys
				val hash = new scala.collection.mutable.HashMap[String, scala.collection.mutable.ArrayBuffer[String]]()
				for(domain <- popDomain.toArray){
					hash.+=((domain, new scala.collection.mutable.ArrayBuffer[String]))
				}
				for(domain <- domainArr){
					val domainBuffer = new scala.collection.mutable.StringBuilder()
					domainBuffer.append(domain)
					if(!domain.endsWith("."))
						domainBuffer.append(".")
					for(candidate <- popDomain){
						val distance = new DLDistance().distance(domainBuffer.toString, candidate)
	  					if(distance > 0 && distance <= 2){
	  						var index = 0
	  						var flag = true
	  						while(index < hash.apply(candidate).length){
	  							breakable{
	  								if(hash.apply(candidate).apply(index) == domainBuffer.toString){
	  									flag = false
	  									break
	  								}	  								
	  							}
	  							index+=1
	  						}
	  						if(flag){
	  							hash.apply(candidate).+=(domainBuffer.toString)
	  						}
	  					}
					}
				}

				//Rewrite all ArrayBuffers in hash map in to the file
				val outFileWriter = new FileWriter(outFileDir + "/" + file.getName, false)
				val outFileBufferWriter = new BufferedWriter(outFileWriter)
				for(domain <- hash.keySet){
					val str = hash.apply(domain).mkString(" ")
					outFileBufferWriter.write(domain+" "+str+"\n")
				}
				outFileBufferWriter.close
			}
		}
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

	def getPairsFromFile(sc: SparkContext, typoArr: Array[String], inFile: File, outFileBufferWriter: BufferedWriter): String = {

		

		println(inFile.toString)
		val domain = typoArr.apply(0) //the correct domain of target website

		// All records in the inFile 
		val dnsRecords = sc.textFile(inFile.toString, 20).map(x => {
			new ParseDNSFast().convert(x)
		})

		if (dnsRecords.count == 0)
			return ""

		val firstRecord = dnsRecords.first

		// All records for correct domain
		val domainArr = dnsRecords.filter(r => {
			val tmp = new parseUtils().parseDomain(r._5, domain)
			tmp == domain
		}).toArray

		for(domainRecord <- domainArr){
			val time = domainRecord._1
			val src_ip = domainRecord._3

			//require all dns records in dnsRecords are sorted by timestamp
			val windowRdd = dnsRecords.mapPartitions(itr => itr.takeWhile(
				_._1 < time //The query for a typo domain must be present before the correct version
			)).cache

			val resultRdd = windowRdd.filter(r => {
				val duration = time - r._1 
				val flag = compareIpAddr(r._3, src_ip)
				flag && duration <= 60
			}).cache
			if (resultRdd.count != 0){
				for( typo <- typoArr) {
					if(typo != domain){
						val rdd = resultRdd.filter(r => {
							val tmp = new parseUtils().parseDomain(r._5, typo)
							tmp == typo
						})
						if(rdd.count!=0){
							val tmpArr = rdd.toArray
							for(typoRecord <- tmpArr){
								val str1 = new ParseDNSFast().antiConvert(typoRecord)
								val str2 = new ParseDNSFast().antiConvert(domainRecord)
								outFileBufferWriter.write(str1+";;"+str2+"\n")
							}
						}
					}
				}
			}

			
		}
		//If there is a query for correct version at the very beginning of this file (within 60sec)
		//it is possible that there are typo qureies for this at the previous file, the left time and 
		//the first record for correct version should be return.
		if((domainArr.apply(0)._1 - firstRecord._1) < 60){
			val duration = 60.toLong - (domainArr.apply(0)._1 - firstRecord._1)
			val str = new ParseDNSFast().antiConvert(domainArr.apply(0))
			return str
		}
		else{
			return ""
		}
	}

	//Only get the records which can get a pair with one record in the last few seconds of one file 
	def getLastPairsFromFile(sc: SparkContext, typoArr: Array[String], inFile: File, outFileBufferWriter: BufferedWriter, line: String): Unit = {
		val dnsRecords = sc.textFile(inFile.toString, 20).map(x => {
			new ParseDNSFast().convert(x)
		}).map(x => (x._1, x))

		if (dnsRecords.count == 0)
			return

		val sortedRecords = dnsRecords.sortByKey(false).map(x => x._2)

		val domainRecord = new ParseDNSFast().convert(line)
		val domain = typoArr.apply(0)
		val time = domainRecord._1
		val src_ip = domainRecord._3

		//require all dns records in dnsRecords are sorted by timestamp
		val windowRdd = sortedRecords.mapPartitions(itr => itr.takeWhile(
			_._1 > time - 60 //The query for a typo domain must be present before the correct version
		))

		val resultRdd = windowRdd.filter(r => { 
				val flag = compareIpAddr(r._3, src_ip)
				flag
			})
			if (resultRdd.count != 0){
				for( typo <- typoArr) {
					if(typo != domain){
						val rdd = resultRdd.filter(r => {
							val tmp = new parseUtils().parseDomain(r._5, typo)
							tmp == typo
						})
						if(rdd.count!=0){
							val tmpArr = rdd.toArray
							for(typoRecord <- tmpArr){
								val str1 = new ParseDNSFast().antiConvert(typoRecord)
								outFileBufferWriter.write(str1+";;"+line)
							}
						}
					}
				}
			}


	}

	def getAllPairs(sc: SparkContext, inFilePath: String, outFilePath: String) = {
		val domainAndTypo = "./res/domainAndTypo/"
		val domainName = convertFilename(inFilePath)

		val typoFile = new File(domainAndTypo+domainName)
		if(typoFile.exists()){

			val outFile = new File(outFilePath)
			if(outFile.exists){
				outFile.delete
			}
			

			val typoArr = io.Source.fromFile(domainAndTypo+domainName).getLines.map(x => x.split(" ")).toArray.apply(0)

			val dir = new File(inFilePath)
			val previousFile = new scala.collection.mutable.StringBuilder()
			if(dir.exists()){
				val filesArr = new ListFiles().recursiveListFiles(dir, new Regex("^part-000*"))
				var index=0
				val logFile = new File("/Users/edmond/log.txt")
				if(logFile.exists){
					logFile.delete
				}
				
				for(file <- filesArr){
					val writer = new FileWriter("/Users/edmond/log.txt", true)
					val writerbuffer = new BufferedWriter(writer)
					writerbuffer.write(file.getAbsolutePath+"\n")
      				writerbuffer.close

					// in one File
					val outFileWrite = new FileWriter(outFilePath, true)
					val outFileBufferWriter = new BufferedWriter(outFileWrite)
					val line = getPairsFromFile(sc, typoArr, file, outFileBufferWriter)
					
					//Deal with the records between two files
					if(index != 0 && line != ""){
						getLastPairsFromFile(sc, typoArr, file, outFileBufferWriter, line)
					}
					previousFile.clear
					previousFile.append(file.toString)
					outFileBufferWriter.close
					
					index += 1
				}
			}
		}
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

		val inFileDir = "./webfiles/"
		val outFileDir = "./res/"
		val outFileDir2 = "./res/pairRecords/"

		val outFileStringBuilder = new scala.collection.mutable.StringBuilder()
		outFileStringBuilder.append(outFileDir)
		outFileStringBuilder.append(args.apply(1))
		if(!outFileStringBuilder.toString.endsWith("/"))
			outFileStringBuilder.append("/")
		val outFile = new File(outFileStringBuilder.toString)
		if(!outFile.exists())
			outFile.mkdir()

		val outFile2 = new File(outFileDir2)
		if(!outFile2.exists())
			outFile2.mkdir()

		//cleanAndDivide(sc)
		//preprocessingAll(sc, "./res/domainAndTypo/")
		sortedDataViaTime(sc, inFileDir+args.apply(0), outFileStringBuilder.toString+args.apply(0))
		getAllPairs(sc, outFileStringBuilder.toString+args.apply(0), outFileDir2+args.apply(0))
	}
	
}