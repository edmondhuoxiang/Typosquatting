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
	def main(args: Array[String]): Unit = {
		System.setProperty("spark.default.parallelism","500")
	  	Logger.getLogger("spark").setLevel(Level.INFO)

	  	val sparkHome = "/Users/edmond/spark-0.7.3"
	  	val jarFile = "target/scala-2.9.3/dnsudf_spark_2.9.3-0.0.jar"
	  	val master = "local[20]"
		val sc = new SparkContext(master, "dnsudf_spark", sparkHome, Seq(jarFile))
	  	val inputPath = "./webfiles"
	  	val outPath = "./res/"

	  	val dir = new File(inputPath)
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
	  	arr.foreach(println)
	}
}