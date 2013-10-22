package org.edmond.webs

import org.edmond.dnsproc_spark._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.edmond.utils._
import org.edmond.DLDistance._

import scala.io.Source
import scala.io._
import java.io.File
import java.io.FileWriter
import java.io.BufferedWriter
import java.io.IOException
import scala.util.control.Breaks._

import spark.SparkContext
import spark.SparkContext._

object test extends Serializable {

	def dealWithZoneFiles(sc: SparkContext) = {
		val zoneFiles = "/data1/sie/zonefiles/com.zone-20130101"
		val redundantFiles = "./res/redundantZones"
		val distinctDir = "./res/distinctZones"

		//Read one big file from disk
		val comZones = sc.textFile(zoneFiles)

		//Get needed field (redundeant) by filtering and mapping
		val redundantZones = comZones.filter(line => {
			val lineArr = line.split(" ")
			if(lineArr.length < 2)
				false
			else
			lineArr.apply(1) == "NS" && lineArr.apply(0) != ""
		}).map(line => {
			val lineArr = line.split(" ")
			lineArr.apply(0)
		})

		//Save into several files
		redundantZones.saveAsTextFile(redundantFiles)

		val targetDir = new File(distinctDir)
		//if The target dir doesn't exist, then create it
		if(!targetDir.exists()){
			targetDir.mkdir()
		}
		//get the list of redundantFiles
		val r = new scala.util.matching.Regex("^part-*")
		val filesArr = new ListFiles().recursiveListFiles(new File(redundantFiles), r)
		//deal with each file
		for(file <- filesArr){
			val tmpRDD = sc.textFile(file.getAbsoluteFile.toString)
			//Create file's handle
			val outFile = new File(distinctDir+"/"+file.getName)
			val outFileWriter = new FileWriter(outFile.getAbsoluteFile,false)
			val outFileBufferWriter = new BufferedWriter(outFileWriter)
			tmpRDD.distinct.toArray.foreach(line => outFileBufferWriter.write(line+"\n"))
			outFileBufferWriter.close
		}


	}
	def isDomainInWebArray(domain: String, webArr: Array[String]): Array[String] = {
		val result = new scala.collection.mutable.ArrayBuffer[String]()
		var index = 0
		while(index < webArr.length){
			val fullName = webArr.apply(index)
			val domainName = fullName.split('.').apply(0)
			if(domainName == domain)
				result.+=(fullName)
			index+=1
		}
		return result.toArray
	}

	def generateDomainTypoList(sc: SparkContext): Unit = {
		val distinctFilesDir = "/Users/edmond/Typosquatting/res/distinctZones"
		val webFiles = "/Users/edmond/Typosquatting/weblist/500_1000"
		val domainFilesDir = "/Users/edmond/Typosquatting/webfiles/"
		val outDir = "./res/"

		val outFileName = "domainAndTypo"
		//val comDomain = "comDomain"
		val outFile = new File(outDir+outFileName)
		if(!outFile.exists()){
			outFile.mkdir()
		}
		//val outFileWriter = new FileWriter(outFile.getAbsoluteFile, false)
		//val outFileBufferWriter = new BufferedWriter(outFileWriter)

		val popWebsites = sc.textFile(webFiles).toArray

		val disinctFiles = new ListFiles().recursiveListFiles(new File(distinctFilesDir))
		for(file <- disinctFiles){
			println(file.toString)
			val domainRDD = sc.textFile(file.toString)
			domainRDD.foreach(domain => {
				println(domain)
				val domainArr = isDomainInWebArray(domain.toLowerCase, popWebsites)
				if(domainArr.length > 0){
					val outFileWriter = new FileWriter(outFile.getAbsoluteFile + "/" + domain, false)
					val outFileBufferWriter = new BufferedWriter(outFileWriter)
				
					for(domainName <- domainArr){
						//check whether the file is existed
						val inFile = new File(domainFilesDir+domainName)
						if(inFile.exists()){
							try { 
						  		//open file
								//val records = sc.textFile(domainFilesDir+domainName).map(x => new ParseDNSFast().convert(x)).map(r=>r._5).distinct
								val records = io.Source.fromFile(domainFilesDir+domainName).getLines.map(x => new ParseDNSFast().convert(x)).map(r=>r._5).toSeq.distinct

								records.foreach(println)
								val resultBuffer = new scala.collection.mutable.StringBuilder()
								resultBuffer.append(domainName)
								val recordsArr = records.toArray
								var j = 0
								while(j < recordsArr.length){
									resultBuffer.append(" "+recordsArr.apply(j))
									j+=1
								}
								resultBuffer.append("\n")
								
								outFileBufferWriter.write(resultBuffer.toString + "\n")
							} catch {
								case e: Exception => 
							}
						}
					}
					outFileBufferWriter.close
				}
			})
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


	def main(args: Array[String]): Unit = {
		System.setProperty("spark.default.parallelism","500")
	  	Logger.getLogger("spark").setLevel(Level.INFO)

	  	val sparkHome = "/Users/edmond/spark-0.7.3"
	  	val jarFile = "target/scala-2.9.3/dnsudf_spark_2.9.3-0.0.jar"
	  	val master = "local[20]"
		val sc = new SparkContext(master, "dnsudf_spark", sparkHome, Seq(jarFile))
	  	val inputPath = "./webfiles"
	  	val outPath = "./res/"

	  	//dealWithZoneFiles(sc)
	  	//generateDomainTypoList(sc)
	  	cleanAndDivide(sc)



	  /*	val dir = new File(inputPath)
	 	val files = new ListFiles().recursiveListFiles(dir)
	  	val numPerTime = 4

	  	val pairOfDomain = new scala.collection.mutable.ArrayBuffer[(String, String)]()
	  	val arr = new scala.collection.mutable.ArrayBuffer[Float]()
	  	val filename = new File(outPath+"result.txt")

	  	if(!filename.exists()){
	  		filename.createNewFile()
	  	}
	  	val fileWriter_summary = new FileWriter(filename.getAbsoluteFile(), false)
	  	val bufferwriter_summary = new BufferedWriter(fileWriter_summary)

	  	var i=0
	  	while(i < files.length){

	  		//ceate output file for each input file
	  		val hostname = files.apply(i).toString.split("/").apply(2)
	  		val file = new File(outPath + hostname)

	  		if(!file.exists()){
	  				file.createNewFile()
	  			}
	  		val fileWriter = new FileWriter(file.getAbsoluteFile(),true);
	  		val bufferwriter = new BufferedWriter(fileWriter);

	  		//Read data from input file
	  		var original_data = sc.textFile(files.apply(i).toString)
	  		val recordData = original_data.map(x => new ParseDNSFast().convert(x))
	  		var data = recordData.map(x => {
	  			new parseUtils().parseDomain(x._5, hostname+".")
	  			})
	  		
	  		//All distinct domain name appeared in the current file is saved in domainArr
	  		val domainArr = data.distinct().filter(r=>{
	  			val distance = new DLDistance().distance(r, hostname+".")
	  			distance <= 2
	  			}).toArray()

	  		val domainCount = collection.mutable.Map[String, Long]()
	  		val domainPairCount = collection.mutable.Map[String, collection.mutable.Map[String, Long]]()

	  		var index=0

	  		bufferwriter.write("Host Name: " + hostname+".\n");
	  		bufferwriter.write("Count of distinct webpage: " + domainArr.length + "\n")
	  		
	  		val countTotal = data.filter(r=>{
	  			val distance = new DLDistance().distance(r, hostname+".")
	  			distance <= 2
	  			}).count()
	  		bufferwriter.write("Total Record: " + countTotal + "\n")
	  		//val list = new scala.collection.mutable.ArrayBuffer[(String, Long)]()
	  		while(index < domainArr.length){
	  			val count1 = data.filter(r => r == domainArr.apply(index)).count
	  			domainCount.+=((domainArr.apply(index),count1))
	  			//println(domainArr.apply(index)+": "+(count1.toFloat/countTotal.toFloat))
	  			index+=1
	  		}
	  		// Get the distribution of each Domain in this file
	  		index = 0
	  		val keySet = domainCount.keySet
	  		keySet.foreach(r =>{
	  			val tmp = domainCount(r).toFloat
	  			bufferwriter.write(r + ":" + (tmp)+"\t")
	  			index+=1
	  			if(index % 7 == 0){
	  				bufferwriter.write("\n")
	  			}
	  			})
	  		bufferwriter.write("\n")



	  		keySet.toArray.foreach(key => {

	  		//	bufferwriter.write("key:"+key+" ")

	  			val compress = recordData.filter(x => {
	  				val tmp = new parseUtils().parseDomain(x._5, key)
	  				tmp == key})
	  		//	bufferwriter.write(compress.count+" records for "+key+"\n")
	  			val count_t = compress.count
	  			val timeLimit = compress.map(x => (x._1, x._3)).toArray
	  			

	  			index = 0
	  			val tmpArr = collection.mutable.Map[String, Long]()
	  			while(index < domainArr.length){
	  				if(domainArr.apply(index) != key){
	  					val tmpSet = recordData.filter(x => {
	  						val tmp = new parseUtils().parseDomain(x._5, key)
	  						tmp == domainArr.apply(index)
	  						}).filter(r => {
	  						var j=0
	  						var flag = false
	  						while(j < timeLimit.length){
	  							val duration = r._1 - timeLimit.apply(j)._1
	  							if(duration > 0 && duration < 60 && timeLimit.apply(j)._2 == r._3){
	  								flag = true
	  							}
	  							j += 1
	  						}
	  						flag
	  						})
	  					val tmpCount = tmpSet.count
	  				//	bufferwriter.write(domainArr.apply(index)+","+tmpCount+" ")
	  					//if(tmpCount!=0)
	  					tmpArr.+=((domainArr.apply(index), tmpCount))
	  				}
	  			//	bufferwriter.write("\n")
	  				index+=1
	  			}
	  			//if(tmpArr.length > 0)
	  			domainPairCount.+=((key, tmpArr))
	  		})

	  		val tmpSet = domainPairCount.keySet
	  		//println("keySet")
	  		//tmpSet.foreach(println)
	  		tmpSet.foreach(d => {
	  			val tmpArr = domainPairCount(d)
	  			var tmpSet = tmpArr.keySet
	  			tmpSet.foreach(domain => {
	  				bufferwriter.write(d +"->"+domain + ":"+tmpArr(domain)+"\t")
	  				})
	  			bufferwriter.write("\n")
	  			})


	  		//All data needed is stored in domainPairCount and domainCount
	  		val tmpArr = new scala.collection.mutable.ArrayBuffer[(String, Long)]()
	  		var index_i = 0
	  		while(index_i < domainArr.length){
	  			val domain_1 = domainArr.apply(index_i)
	  			var count_4 = 0.toLong
	  			var tmpSet = domainPairCount.keySet
	  			tmpSet.foreach(tmpDomain =>{
	  			if(tmpDomain!=domain_1 && domainPairCount(tmpDomain)(domain_1)!=0)
	  				count_4 += 1
	  			})
	  			tmpArr.+=((domain_1, count_4))
	  			index_i+=1
	  		}
	  		val sortedForRes4 = tmpArr.sortWith(_._2 > _._2)
	  		index_i =0
	  		while(index_i < domainArr.length){
	  			val domain_1 = domainArr.apply(index_i)

	  			var index_j = 0
	  			//val average = new scala.collection.mutable.ArrayBuffer[(String, Long)]()
	  			var average = 0.toFloat
	  			while(index_j < domainArr.length){
	  				if(index_i!=index_j){
	  					val domain_2 = domainArr.apply(index_j)

	  					var count_2 = 0
	  					domainPairCount(domain_2).keySet.foreach(tmpDomain => {
	  						if(domainPairCount(domain_2)(tmpDomain)!=0)
	  							count_2+=1
	  						})
	  					average += count_2
	  				}
	  				index_j+=1
	  			}
	  			average = average/(domainArr.length-1)
	  			if(average < 1)
	  				average = 1.toFloat

	  			index_j = 0
	  			while(index_j < domainArr.length){
	  				if(index_j!=index_i){
	  					val domain_2 = domainArr.apply(index_j)

	  					var tmpCount = domainPairCount(domain_2)(domain_1)

	  					//P(domain_2 -> domain_1 | domain_2)
	  					var count_1 = tmpCount.toFloat/domainCount(domain_2)

	  					//P(domain_2 -> X | domain_2)
	  					var count_1_sum = 0.toLong
	  					domainPairCount.keySet.foreach(tmpDomain => {
	  						if(tmpDomain != domain_2)
	  						count_1_sum += domainPairCount(domain_2)(tmpDomain)
	  						})
	  					var count_1_res = count_1.toFloat / (count_1_sum.toFloat / domainCount(domain_2))


	  					//|P(domain_2 -> X | domain_2)|
	  					var count_2 = 0
	  					domainPairCount(domain_2).keySet.foreach(tmpDomain => {
	  						if(domainPairCount(domain_2)(tmpDomain)!=0)
	  							count_2+=1
	  						})

	  					//P(domain_2 -> domain_1 | domain_1)
	  					var count_3 = tmpCount.toFloat/domainCount(domain_1)

	  					//P(X -> domain_1 | domain1)
	  					var count_3_sum = 0.toLong
	  					domainPairCount.keySet.foreach(tmpDomain => {
	  						if(tmpDomain != domain_1)
	  							count_3_sum += domainPairCount(tmpDomain)(domain_1)
	  						})
	  					var count_3_res = 0.toFloat
	  					if(count_3_sum == 0){
	  						count_3_res = 0.toFloat
	  					}
	  					else{
	  						count_3_res = count_3.toFloat / (count_3_sum.toFloat/domainCount(domain_1))
	  					}

	  					//|P(X -> domain_1 | domain_1)|
	  					var count_4 = 0.toLong
	  					var tmpSet = domainPairCount.keySet
	  					tmpSet.foreach(tmpDomain =>{
	  						if(tmpDomain!=domain_1 && domainPairCount(tmpDomain)(domain_1)!=0)
	  							count_4 += 1
	  						})
	  					var count_4_res = 0
	  					breakable{
	  						while(count_4_res < sortedForRes4.size){
	  							if(sortedForRes4.apply(count_4_res)._1 == domain_1)
	  								break
	  							count_4_res+=1
	  						}
	  					}
	  					
	  					if(count_1_res>=0.5 && count_2 <= 3 && count_4_res <= 3){
	  						arr.+=(count_4)
	  						pairOfDomain.+=((domain_2,domain_1))
	  					}
	  				}
	  				index_j+=1
	  			}
	  			index_i+=1
	  		}
	  		i+=1
	  		bufferwriter.close
	  	}
	  	
	  	pairOfDomain.foreach(r => {
	  		println("("+r._1+","+r._2+")")
	  		bufferwriter_summary.write("("+r._1+","+r._2+")\n")
	  		})
	  	bufferwriter_summary.close
	  	arr.foreach(println)*/
	}
}