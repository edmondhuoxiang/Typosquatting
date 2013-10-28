package org.edmond.webs

import org.edmond.DLDistance._
import org.edmond.utils._
import org.edmond.Partitioner._
import org.edmond.dnsproc_spark._
import org.apache.log4j.Logger
import org.apache.log4j.Level


import spark.SparkContext
import spark.SparkContext._
import spark.PairRDDFunctions
import spark.OrderedRDDFunctions

import util.control.Breaks._
import scala.io.Source
import scala.io._
import java.io.File
import java.io.FileWriter
import java.io.BufferedWriter
import java.io.IOException

object webs extends Serializable {
	def not_main(args: Array[String]): Unit = {

	  System.setProperty("spark.default.parallelism","500")
	  Logger.getLogger("spark").setLevel(Level.INFO)

	  val sparkHome = "/Users/edmond/spark-0.7.3"
	  val jarFile = "target/scala-2.9.3/dnsudf_spark_2.9.3-0.0.jar"
	  val master = "local[20]"
	  

	  val outPath = "./res/"
	  val dataPath = "/data1/sie/ch202/201212/"

	  //Reading correct domain name from file
	  val sourceFile = "./weblist/500_1000"
	  //val sourceFile = "./weblist/test.file"

	  //Source file dir and regex
	  val dir = new File(dataPath)
	  val r = new scala.util.matching.Regex("(^raw_processed.201212).*(0.gz$)")
	  val files = new ListFiles().recursiveListFiles(dir, r)
	  val numPerTime = 4


	  var index = 0
	  val sc = new SparkContext(master, "dnsudf_spark", sparkHome, Seq(jarFile))
	  val webList = sc.textFile(sourceFile)
	  val partitioner = new HashPartitioner(webList.count.asInstanceOf[Int]*2)
	  val sortFunc = new OrderedRDDFunctions(webList.map(r => (r, 1)))
	  val sortedList = sortFunc.sortByKey()
	  val arrWeb = sortedList.map(r => r._1).toArray

	  while(index < files.length){
	  	//println(files.apply(index))
	  	var num=0
	  	if(index+numPerTime < files.length){
	  		num = numPerTime
	  	}
	  	else{
	  		num = files.length - index;
	  	}
	  	
	  	println("Processing files from " +index+ " to " + (index+num-1));
	  	//Read DNS records
	  	var original_data = sc.textFile(files.apply(index).toString)

	  	for(i <- 1 until num){
	  		original_data=original_data.++(sc.textFile(files.apply(index+i).toString))
	  	}
	  	
	  	//run code here
	  	//Parse answer
	  	val data = original_data.map(x => new ParseDNSFast().convert(x))

	  	
	  	val data_pair = data.map(r => {
	  	val index1 = new parseUtils().lookUpString(r._5, arrWeb, 0, arrWeb.length-1)
	  	if(index1 > -1){
	  		val oneDomain = arrWeb.apply(index1)
	  		//println(oneDomain + " " + r._5)
	  		(oneDomain, r)
	  	} else {
	  		val oneDomain = "NOTHING"
	  		(oneDomain, r)
	  	}
	  	}).filter(r => (!r._1.contentEquals("NOTHING")))



	  	//data_pair.foreach(println)
	  //println("There are totally " + data_pair.count + " records.")

	  	var func = new PairRDDFunctions(data_pair)
	  	val data_partitions = func.partitionBy(partitioner)

	 	data_partitions.foreachPartition(r => {
	  		if(r.nonEmpty){
	  			val filename = r.take(1).toArray.apply(0)._1
	  			println("Filename: " + filename)
	  			val file = new File(outPath + filename)
	  			//if file doesn't exists, then create it
	  			if(!file.exists()){
	  				file.createNewFile()
	  			}
	  			val fileWriter = new FileWriter(file.getAbsoluteFile(),true);
	  			val bufferwriter = new BufferedWriter(fileWriter);
	  			
	  			//val outFile = new java.io.FileWriter(outPath + filename)
	  			r.map(record => new ParseDNSFast().antiConvert(record._2)).foreach(r => bufferwriter.write(r+"\n"))
	  			//outFile.close
	  			bufferwriter.close
	  		}
	  	})

	  	//end code

	  	index+=num
	  }
/*

	  val sc = new SparkContext(master, "dnsudf_spark", sparkHome, Seq(jarFile))
	  
	  

	   
	  //val original_data = sc.textFile("/data1/sie/ch202/201212/raw_processed.201212*.0.gz")
	  val original_data = sc.textFile("/data1/sie/ch202/201212/raw_processed.20121201.0100.1354323600.064745979.0.gz")
	  //Parse answer
	  val data = original_data.map(x => new ParseDNSFast().convert(x))

	  val partitioner = new HashPartitioner(webList.count.asInstanceOf[Int])
	  val arrWeb = sortedList.map(r => r._1).toArray
	  val data_pair = data.map(r => {
	
	  	val index = new parseUtils().lookUpString(r._5, arrWeb, 0, arrWeb.length-1)
	  	if(index > -1){
	  		val oneDomain = arrWeb.apply(index)
	  		//println(oneDomain + " " + r._5)
	  		(oneDomain, r)
	  	} else {
	  		val oneDomain = "NOTHING"
	  		(oneDomain, r)
	  	}
	  }).filter(r => (!r._1.contentEquals("NOTHING")))

	  //data_pair.foreach(println)
	  //println("There are totally " + data_pair.count + " records.")

	  var func = new PairRDDFunctions(data_pair)
	  val data_partitions = func.partitionBy(partitioner)

	  data_partitions.foreachPartition(r => {
	  		if(r.nonEmpty){
	  			val filename = r.take(1).toArray.apply(0)._1
	  			println("Filename: " + filename)
	  			val file = new File(outPath + filename)
	  			//if file doesn't exists, then create it
	  			if(!file.exists()){
	  				file.createNewFile()
	  			}
	  			val fileWriter = new FileWriter(file.getAbsoluteFile(),true);
	  			val bufferwriter = new BufferedWriter(fileWriter);
	  			
	  			//val outFile = new java.io.FileWriter(outPath + filename)
	  			r.map(record => new ParseDNSFast().antiConvert(record._2)).foreach(r => bufferwriter.write(r+"\n"))
	  			//outFile.close
	  			bufferwriter.close
	  		}
	  	})*/
	}
}