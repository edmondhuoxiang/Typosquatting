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

	def isDomainInWebArray(domain: String, webArr: Array[String]): Boolean = {
		for(name <- webArr){
			if (domain == name + "."){
				return true
			}
		}
		return false
	}

	def getDomainAndTypo(sc: SparkContext, inFile: String, outFile: String) = {
		
		println("inFile: "+inFile)
		println("outFile: "+outFile)
		val popWebFile = "./weblist/top1000"

		//Reading popWeb list from file
		val popWebsites = sc.textFile(popWebFile).toArray.toList

		val dnsRecords = sc.textFile(inFile).map(x => new ParseDNSFast().convert(x))
		val filename = inFile.split('/').toList.last
		val domain = filename+"."

		val domainsRdd = dnsRecords.map(r => {
			val domainName = new parseUtils().parseDomain(r._5, domain)
			domainName
		}).distinct.filter(_ != domain)

		val unPopDomain = domainsRdd.filter(name => !isDomainInWebArray(name.toLowerCase, popWebsites.toArray)).toArray

		val outFileWriter = new FileWriter(outFile, false)
		val outFileBufferWriter = new BufferedWriter(outFileWriter)
		outFileBufferWriter.write(domain)
		if(unPopDomain.length!=0){
			outFileBufferWriter.write(" ")
			for(name <- unPopDomain){
				outFileBufferWriter.write(name + " ")
			}
		}
		outFileBufferWriter.close
	}


	def sortedDataViaTime(sc: SparkContext, inFilePath: String, outFilePath: String) = {

		println(inFilePath)
		println(outFilePath)
		val tmp  = sc.textFile(inFilePath, 20)
		val count = tmp.count
		val num = count/10000
		val data = sc.textFile(inFilePath, num.toInt)

		val dnsRecords = data.map(x => {
			new ParseDNSFast().convert(x)
		}).map(x => (x._1, x))

		val sortedRecords = dnsRecords.sortByKey().map(x => x._2)

		sortedRecords.map(x => new ParseDNSFast().antiConvert(x)).saveAsTextFile(outFilePath)
	}

	def convertFilename(str: String): String = {
		val tmp = str.split('/')
		val filename = tmp.apply(tmp.length-2)
		val filenameArr = filename.split('.')
		val outFilename = new scala.collection.mutable.StringBuilder()
		var index = 0
	/*	for(string <- filenameArr){
			if(index != filenameArr.length-1){
				outFilename.append(filenameArr.apply(index).toUpperCase)
				if(index != filenameArr.length-2)
					outFilename.append(".")
			}
			index+=1
		}

		return outFilename.toString*/

		return filenameArr.apply(0).toUpperCase
	}

	def preprocessing(sc: SparkContext, inFilePath: String): Unit = {
		println(inFilePath)
		val data = sc.textFile(inFilePath,20).map(line => line.split(" ")).toArray
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

		val popWebsites = sc.textFile(webFiles, 20).toArray

		//get list of all files
		val fileList = new ListFiles().recursiveListFiles(new File(inFileDir))
		for(file <- fileList){
			println(file.toString)
			val records = sc.textFile(file.toString, 20).toArray
			if(records.length > 0){
				for(line <- records){			
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
					for(domain <- hash.keySet){
						val filename = domain.split('.').apply(0).toUpperCase
						println("FILENAME: "+filename)
						val outFileWriter = new FileWriter(outFileDir + "/" + filename, false)
						val outFileBufferWriter = new BufferedWriter(outFileWriter)
						val str = hash.apply(domain).mkString(" ")
						outFileBufferWriter.write(domain+" "+str+"\n")
						outFileBufferWriter.close
					}
					
				}
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

	def getQueryForDomain(sc: SparkContext, domain: String, dnsRecords: spark.RDD[(Int, Int, String, String, String, Int, Int, List[Array[String]], List[Array[String]], List[Array[String]])], outFile: String) = {
		
		val resArr = dnsRecords.filter(x => {
			val tmp = new parseUtils().parseDomain(x._5,domain)
			tmp == domain
		}).toArray

		if (resArr.length != 0){
			val outFileWriter = new FileWriter(outFile, true)
			val outFileBufferWriter = new BufferedWriter(outFileWriter)

			for(record <- resArr){
				val line = new ParseDNSFast().antiConvert(record)
				outFileBufferWriter.write(line+"\n")
			}
			outFileBufferWriter.close
		}
	}

	def getQueriesoforAllDomains(sc: SparkContext, inFile: String, outFile: String): Unit = {
		
		val writer = new FileWriter("./log.txt", true)
		val writerbuffer = new BufferedWriter(writer)
		writerbuffer.write(inFile+" ")
		writerbuffer.flush
      	val t1 = System.nanoTime()

		println("***********Here we go***************")
		val domainAndTypo = "./res/domainAndTypo/"
	/*	val domainName = convertFilename(inFile)
		val tmpArr = inFile.split('/')
		val filename = tmpArr.apply(tmpArr.length-1)
		val domain = tmpArr.apply(tmpArr.length-2)+"."
	*/
		val tmpArr = inFile.split('/')
		val domain = tmpArr.apply(tmpArr.length-1)+"."
		val domainName = domain.split('.').apply(0).toUpperCase

		println("inFile: "+inFile)
		println("domainName: "+domainName)
	//	println("filename: "+filename)
		val file = new File(domainAndTypo+domainName)
		if(!file.exists){
			return
		}

		val outDir = new File(outFile)
		if(!outDir.exists){
			outDir.mkdir
		}

		val typoRdd = sc.textFile(domainAndTypo+domainName, 20).flatMap(x => x.split(' ')).filter(_ != domain)
		val dnsRecords = sc.textFile(inFile, 20).map(x => new ParseDNSFast().convert(x)).toArray.toList
		val res = typoRdd.map(typo => {
			val resList = dnsRecords.filter(x =>{
				val tmp = new parseUtils().parseDomain(x._5, typo)
				tmp == typo
			})
			(typo, resList)
		})


		res.foreach(r => {
			val typo = r._1
			val list = r._2
			if(list.length != 0){
				val outPath = outFile+"/"+typo
				val outFileWriter = new FileWriter(outPath, true)
				val outFileBufferWriter = new BufferedWriter(outFileWriter)

				for(r <- list){
					val line = new ParseDNSFast().antiConvert(r)
					outFileBufferWriter.write(line+"\n")
				}
				outFileBufferWriter.close
			}
			println("FINISH "+typo)
		})



		//////////////////////
/*
		val typoArr = io.Source.fromFile(domainAndTypo+domainName).getLines.map(x => x.split(" ")).toArray.apply(0)

		

		val outDir = new File(outFile)
		if(!outDir.exists){
			outDir.mkdir
		}

		val target = typoArr.apply(0)
		for(typo <- typoArr){
			if(typo!=target){
				val dnsRecords = sc.textFile(inFile, 20).map( x =>{
					new ParseDNSFast().convert(x)
				}).cache
				getQueryForDomain(sc, typo, dnsRecords, outFile+"/"+typo)
			}
		}

*/

		val t2 = System.nanoTime()
		val micros = (t2 - t1) / 1000
		writerbuffer.write("%d microseconds".format(micros) + "\n")
      	writerbuffer.close
	}
//
	def getTTL(record: (Int, Int, String, String, String, Int, Int, List[Array[String]],List[Array[String]],List[Array[String]])): Int = {
		val domain = record._5
		val answ_section = record._8
		val anth_section = record._9
		val addi_section = record._10

		for(rr <- answ_section){
			if(rr.apply(0) == domain && rr.apply(3) == "A")
				return rr.apply(1).toInt
		}

		for(rr <- anth_section){
			if(rr.apply(0) == domain && rr.apply(3) == "A")
				return rr.apply(1).toInt
		}

		for(rr <- addi_section){
			if(rr.apply(0) == domain && rr.apply(3) == "A")
				return rr.apply(1).toInt
		}

		return 0
	}

	def getPairsFromFile(sc: SparkContext, typoArr: Array[String], inFile: File, outFileBufferWriter: BufferedWriter): String = {		

		println(inFile.toString)
		val domain = typoArr.apply(0) //the correct domain of target website

		// All records in the inFile 
		val dnsRecords = sc.textFile(inFile.toString, 20).map(x => {
			new ParseDNSFast().convert(x)
		}).filter(x => x._3.contains("."))

		if (dnsRecords.count == 0)
			return ""

		val ips = dnsRecords.map(x => x._3).filter(x => x.contains(".")).map(x => {
			val arr = x.split('.')
			val ip = arr.apply(0)+"."+arr.apply(1)+".0.0"
			ip
		}).distinct

	//	val ips = dnsRecords.map(x => x._3).filter(x => x.contains(".")).distinct
		
		val ttls = dnsRecords.filter(r => {
			val tmp = new parseUtils().parseDomain(r._5, domain)
			val flag = r._3.contains(".")
			tmp == domain && flag
		}).filter(x => {
			val ttl = getTTL(x)
			ttl != 0
		}).toArray


		
		val hashMap = scala.collection.mutable.HashMap[String, scala.collection.mutable.ArrayBuffer[(Int, Int)]]()
		for(ip <- ips.toArray){
			val arr = new scala.collection.mutable.ArrayBuffer[(Int, Int)]()
			hashMap.+=((ip, arr))
		}

		//Add all ttl time window of correct domain into hashmap

		for(ttlRecord <- ttls){
			val time = ttlRecord._1
			val src_ip = ttlRecord._3
			val ttl = getTTL(ttlRecord)

			if(ttl!=0){
				val tmpArr = src_ip.split('.')
				val groupIp = tmpArr.apply(0)+"."+tmpArr.apply(1)+".0.0"
				val tmp = hashMap(groupIp)
				tmp.+=((time, ttl))
			}
		}

		val dnsRecordsFiltered = dnsRecords.filter(x =>{
			val ip = x._3
			val tmpArr = ip.split('.')
			val groupIp = tmpArr.apply(0)+"."+tmpArr.apply(1)+".0.0"
			val timeArr = hashMap(groupIp).toArray
			val time = x._1
			var flag = true

			breakable {
				for(window <- timeArr){
					if(time > window._1 && time < window._1+window._2){
						flag =false
						break
					}
				}
			}
			flag
		}).cache



		val lastRecord = dnsRecords.toArray.toList.last

		val typoRecords = dnsRecordsFiltered.filter(r => {
			val tmp = new parseUtils().parseDomain(r._5, domain)
			val flag = r._3.contains(".")
			tmp != domain && flag
		})

		val dnsArr = dnsRecordsFiltered.toArray.toList

		val res = typoRecords.map(record => {
			val time = record._1
			val src_ip = record._3

			val window = dnsArr.filter(r => {
				val duration = r._1 - time
				val flag = compareIpAddr(r._3, src_ip)
				val tmpDomain = new parseUtils().parseDomain(r._5, domain)
				tmpDomain == domain && flag && duration <= 60 && duration >= 0
			})
			(record, window)
		}).toArray

		for(r <- res){
			val record1 = r._1
			val list = r._2
			for(l <- list){
				val record2 = l
				val str1 = new ParseDNSFast().antiConvert(record1)
				val str2 = new ParseDNSFast().antiConvert(record2)
				outFileBufferWriter.write(str1+";;"+str2+"\n")
			}
		}
		if(res.length == 0)
			return ""

		val lastTypoRecord = res.apply(res.length-1)._1

		if((lastTypoRecord._1 - lastRecord._1) > 60 ) {
			return ""
		}
		else{
			val str = new ParseDNSFast().antiConvert(lastTypoRecord)
			return str
		}
		///////////////////////////////////////
/*

		val typoRecords = dnsRecordsFiltered.filter(r => {
			val tmp = new parseUtils().parseDomain(r._5, domain)
			val flag = r._3.contains(".")
			tmp != domain && flag
		}).toArray
		for(record <- typoRecords){
			val time = record._1
			val src_ip = record._3
			val windowRdd = dnsRecordsFiltered.mapPartitions(itr => itr.takeWhile(
				_._1 < time + 60
			))
			val resultRdd = windowRdd.filter(r => {
				val duration = r._1 - time
				val flag = compareIpAddr(r._3, src_ip)
				val tmpDomain = new parseUtils().parseDomain(r._5, domain)
				tmpDomain == domain && flag && duration <=60 && duration >=0 
			})

			if(resultRdd.count != 0){
				val tmpArr = resultRdd.toArray
				for(record2 <- tmpArr){
					val str1 = new ParseDNSFast().antiConvert(record)
					val str2 = new ParseDNSFast().antiConvert(record2)
					outFileBufferWriter.write(str1+";;"+str2+"\n")			
				}
			}
		}
*/
		//////////////////////////////////////
/*
		// All records for correct domain
		val domainArr = dnsRecords.filter(r => {
			val tmp = new parseUtils().parseDomain(r._5, domain)
			val flag = r._3.contains(".")
			tmp == domain && flag
		}).toArray

		for(domainRecord <- domainArr){
			val time = domainRecord._1
			val src_ip = domainRecord._3
			val ttl = getTTL(domainRecord)
			if((time+ttl)>hashMap(src_ip))
				hashMap(src_ip) = time+ttl
			//require all dns records in dnsRecords are sorted by timestamp
			val windowRdd = dnsRecords.mapPartitions(itr => itr.takeWhile(
				_._1 < time //The query for a typo domain must be present before the correct version
			))

			val resultRdd = windowRdd.filter(r => {
				val duration = time - r._1 
				val flag = compareIpAddr(r._3, src_ip)
				flag && duration <= 60
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
								
								if(typoRecord._1 > hashMap(typoRecord._3)){
									val str1 = new ParseDNSFast().antiConvert(typoRecord)
									val str2 = new ParseDNSFast().antiConvert(domainRecord)
									outFileBufferWriter.write(str1+";;"+str2+"\n")
								}
							}
						}
					}
				}
			}
/*
			val ttl = getTTL(domainRecord)
			//get all records in TTL
			val windowRdd2 = dnsRecords.mapPartitions(itr => itr.takeWhile(
				_._1 <= time+ttl
			)).filter(r => r._1 >= time)
			
			val resultRdd2 = windowRdd2.filter(r => {
				//val flag = compareIpAddr(r._3, src_ip)
				val flag = r._3 == src_ip //Not sure whether two resolvers share their cache
				flag
			}).cache

			if (resultRdd2.count != 0){
				for(typo <- typoArr){
					val rdd = resultRdd2.filter(r => {
						val tmp = new parseUtils().parseDomain(r._5, typo)
						tmp == typo
					})
					if(rdd.count!=0){
						val tmpArr = rdd.toArray
						for(typoRecord <- tmpArr){
							val str1 = new ParseDNSFast().antiConvert(typoRecord)
							val str2 = new ParseDNSFast().antiConvert(domainRecord)
							val str3 = "CACHED BY RESOLVER"
							val str4 = "ENDING TTL WINDOW IN 60 SEC"
							if((time+ttl-typoRecord._1) < 60)
								outFileBufferWriter.write(str1+";;"+str2+";;"+str3+";;"+str4+"\n")
							else
								outFileBufferWriter.write(str1+";;"+str2+";;"+str3+"\n")
						}
					}
				}
			}*/
		}*/
		//If there is a query for correct version at the very beginning of this file (within 60sec)
		//it is possible that there are typo qureies for this at the previous file, the left time and 
		//the first record for correct version should be return.
	/*	if((domainArr.apply(0)._1 - firstRecord._1) < 60){
			val duration = 60.toLong - (domainArr.apply(0)._1 - firstRecord._1)
			val str = new ParseDNSFast().antiConvert(domainArr.apply(0))
			return str
		}
		else{
			return ""
		}*/
	}

	//Only get the records which can get a pair with one record in the last few seconds of one file 
	def getLastPairsFromFile(sc: SparkContext, typoArr: Array[String], inFile: File, outFileBufferWriter: BufferedWriter, line: String): Unit = {
		
		val dnsRecords = sc.textFile(inFile.toString, 20).map(x => {
			new ParseDNSFast().convert(x)
		})

		val typoRecord = new ParseDNSFast().convert(line)
		val domain = typoArr.apply(0)
		val time = typoRecord._1
		val src_ip = typoRecord._3

		val windowRdd = dnsRecords.mapPartitions(itr => itr.takeWhile(
			_._1 < time+60
		))

		val resultRdd = windowRdd.filter(r => {
			val flag = compareIpAddr(r._3, src_ip)
			val tmp = new parseUtils().parseDomain(r._5, domain)
			tmp == domain && flag
		}).toArray

		if(resultRdd.length != 0){
			for(r <- resultRdd){
				val str2 = new ParseDNSFast().antiConvert(r)
				outFileBufferWriter.write(line+";;"+str2)
			}
		}


		///////////////////////
/*
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

*/
	}

	def getAllPairs(sc: SparkContext, inFilePath: String, outFilePath: String) = {
		val domainAndTypo = "./res/domainAndTypo/"
		val domainName = convertFilename(inFilePath)

		val tmpArr = inFilePath.split('/')
		val filename = tmpArr.apply(tmpArr.length-1)

		println("domainName: "+domainName)
		println("filename: "+filename)
		val typoFile = new File(domainAndTypo+domainName)
		if(typoFile.exists()){
/*
			val outFile = new File(outFilePath)
			if(outFile.exists){
				outFile.delete
			}
*/		

			val typoArr = io.Source.fromFile(domainAndTypo+domainName).getLines.map(x => x.split(" ")).toArray.apply(0)

			val dir = new File(inFilePath)
			//val previousFile = new scala.collection.mutable.StringBuilder()
			if(dir.exists()){
				//val filesArr = new ListFiles().recursiveListFiles(dir, new Regex("^part-000*"))
				//var index=0
				val logFile = new File("./log.txt")
				if(logFile.exists){
				//	logFile.delete
				}
				
				val writer = new FileWriter("./log.txt", true)
				val writerbuffer = new BufferedWriter(writer)
				writerbuffer.write(inFilePath+" ")
      			writerbuffer.flush
      			val t1 = System.nanoTime()

				// in one File
				val outFileWrite = new FileWriter(outFilePath, true)
				val outFileBufferWriter = new BufferedWriter(outFileWrite)
				println("******************")
				println("inFilePath: "+inFilePath)
				val infile1 = new File (inFilePath)
				val line = getPairsFromFile(sc, typoArr, infile1, outFileBufferWriter)
				
				//Deal with the records between two files
				val indexStr = filename.split('-')
				val length = indexStr.apply(1).length
				val index = indexStr.apply(1).toInt

				if( line != ""){
					val next = index+1
					val path = inFilePath.subSequence(0, inFilePath.lastIndexOf('/')+1)
					val format = "%0"+length.toString+"d"	
					val nextFilename = "part-"+format.format(next)
					println("Next: "+path+nextFilename)
					println("outFilePath: "+outFilePath)
					println("*****************")
					val infile2 = new File(path + nextFilename)
					if(infile2.exists){
						getLastPairsFromFile(sc, typoArr, infile2, outFileBufferWriter, line)
					}
				}
				outFileBufferWriter.close
/*
				if(index != 0 && line != ""){

					val pre_index = index - 1
					val path = inFilePath.subSequence(0, inFilePath.lastIndexOf('/')+1)
					val format = "%0"+length.toString+"d"
					val pre_filename = "part-"+format.format(pre_index)
					println("PreFileName: "+path+pre_filename)
					println("outFilePath: "+outFilePath)
					println("*****************")
					val infile2 = new File(path + pre_filename)
					getLastPairsFromFile(sc, typoArr, infile2, outFileBufferWriter, line)
				}
				//previousFile.clear
				//previousFile.append(file.toString)
				//outFileBufferWriter.close
					
*/
				val t2 = System.nanoTime()
				val micros = (t2 - t1) / 1000
				writerbuffer.write("%d microseconds".format(micros) + "\n")
      			writerbuffer.close
				
			}
		}
	}


	def isExistedWebsite(domain: String, records: List[(Int, Int, String, String, String, Int, Int, List[Array[String]], List[Array[String]], List[Array[String]])] ): Boolean = {
		val filtedList = records.filter(r => {
			val tmp = new parseUtils().parseDomain(r._5, domain)
			tmp == domain
		})

		val sum = filtedList.map(r => getTTL(r)).sum
		if(sum == 0){
			return false
		}
		else{
			return true
		}
		
	}

	def statistics(sc: SparkContext, inFile: String, outFile: String) = {

		println("inFile: "+inFile)
		println("outFile: "+outFile)

		val domain = inFile.split('/').toList.last

		val pairRecords = sc.textFile(inFile, 20).map(line => {
			val arr = line.split(";;")
			val typoRecord = new ParseDNSFast().convert(arr.apply(0))
			val targetRecord = new ParseDNSFast().convert(arr.apply(1))
			(typoRecord, targetRecord)
		})

		val typoRdd = pairRecords.map(pair => {
			val typoRecord = pair._1
			val typo = typoRecord._5
			typo
		}).distinct //get all distinct typos

		val pairList = pairRecords.toArray.toList

		val resLines = typoRdd.map(typo => {
			val records = pairList.filter(pair => {
				val typoRecord = pair._1
				typo == typoRecord._5
			}) //all pair-records about this typo

			val pair_count = records.length


			val distances = records.map(pair => {
				val typoRecord = pair._1
				val targetRecord = pair._2
				targetRecord._1 - typoRecord._1
			})
			var sum = 0
			for(dist <- distances){
				sum += dist
			}
			val aver = sum.toFloat/pair_count.toFloat

			val target = domain+"."

			val singleRecords = io.Source.fromFile("./res/singleTypoRecord/"+domain+"/"+typo).getLines.toArray.toList.map(r => {
				new ParseDNSFast().convert(r)
			})

			val single_count = singleRecords.length

			val flag = isExistedWebsite(typo, singleRecords)

			val resLine = typo+","+target+","+single_count.toString+","+pair_count+","+aver.toString+","+flag.toString
			resLine
		}).toArray

		val outFileWrite = new FileWriter(outFile, false)
		val outFileBufferWriter = new BufferedWriter(outFileWrite)
		for(line <- resLines){
			outFileBufferWriter.write(line+"\n")
		}
		outFileBufferWriter.close

	}

	def getDistribution(sc: SparkContext, inFile: String, outFile: String) = {
		val records = sc.textFile(inFile, 20).map(line => {
			val arr = line.split(',')
			(arr.apply(0), arr.apply(1), arr.apply(2).toInt, arr.apply(3).toInt, arr.apply(4).toFloat, arr.apply(5).toBoolean)
		}).filter(x => (x._4.toFloat/x._3.toFloat > 0.2))

		val dir = new File("outFile")
		if(!dir.exists){
			dir.mkdir
		}
		val existedList = records.filter(r => r._6).toArray.toList
		val nonExistedList = records.filter(r => !r._6).toArray.toList

		var count1 = 0
		var sum1 = 0.0
		existedList.foreach(r => {
			count1 += r._4
			sum1 += r._4 * r._5
		})

		var count2 = 0
		var sum2 = 0.0
		nonExistedList.foreach(r => {
			count2 += r._4
			sum2 += r._4 * r._5
		})

		val outFileWrite = new FileWriter(outFile, false)
		val outFileBufferWriter = new BufferedWriter(outFileWrite)

		outFileBufferWriter.write("For existed typo domain, there are " + existedList.length + " pairs of typo and target website, " + 
									count1 + " records. The average time is " +(sum1.toFloat/count1.toFloat)+".\n")
		outFileBufferWriter.write("For non-existed typo domain, there are " + nonExistedList.length + " pairs of typo and target website, " + 
									count2 + " records. The average time is " + (sum2.toFloat/count2.toFloat) + ".\n")

		outFileBufferWriter.close

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
		val outFileDir = "./res/"
		
		

		val outFileStringBuilder = new scala.collection.mutable.StringBuilder()
		outFileStringBuilder.append(outFileDir)
		outFileStringBuilder.append(args.apply(1))
		if(!outFileStringBuilder.toString.endsWith("/"))
			outFileStringBuilder.append("/")
		val outFile = new File(outFileStringBuilder.toString)
		if(!outFile.exists())
			outFile.mkdir()
/////////////////////////
		
		val inFileDir = "./res/webfiles/"

		val outFilename = args.apply(0).split('/').toList.last + "."
		
		getDomainAndTypo(sc, inFileDir + args.apply(0), outFileDir+args.apply(1)+outFilename)

////////////////////////

//		cleanAndDivide(sc)
//		preprocessingAll(sc, "./res/domainAndTypo/")

//		val inFileDir = "./webfiles/"	
//		sortedDataViaTime(sc, inFileDir+args.apply(0), outFileStringBuilder.toString+args.apply(0))
////////////////////////
/*
		val inFileDir2 = "./res/sortedWebFiles/"
		println("args(0): "+args.apply(0))
		val dirname = args.apply(0).split('/').apply(0)
		val filename = args.apply(0).split('/').apply(1)
		//println(inFileDir2+args.apply(0))
		//println(outFileDir+args.apply(1)+"/"+dirname)

		getAllPairs(sc, inFileDir2+args.apply(0), outFileDir+args.apply(1)+"/"+dirname)
*/
/////////////////////////
/*
		val outFileDir2 = "./res/singleTypoRecord/"
		val outFile2 = new File(outFileDir2)
		if(!outFile2.exists){
			outFile2.mkdir()
		}
		println(outFileDir2+dirname)
		getQueriesoforAllDomains(sc, inFileDir2+args.apply(0), outFileDir2+dirname)
*/

////////////////////////
		val inFileDir3 = "./res/pairRecords/"
		//statistics(sc, inFileDir3+args.apply(0), outFileStringBuilder.toString+args.apply(0))

///////////////////////
//		getDistribution(sc, "./res/statistics", "./res/summary")

	}
	
}