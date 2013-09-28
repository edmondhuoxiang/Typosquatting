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
	def main(args: Array[String]): Unit = {

	  Logger.getLogger("spark").setLevel(Level.INFO)

	  val sparkHome = "/Users/edmond/spark-0.7.3"
	  val jarFile = "target/scala-2.9.3/dnsudf_spark_2.9.3-0.0.jar"
	  val master = "local[20]"
	  val sc = new SparkContext(master, "dnsudf_spark", sparkHome, Seq(jarFile))

	  val outPath = "./res/"
	  val dataPath = "/data1/sie/ch202/201212/"

	  //Reading correct domain name from file
	  val sourceFile = "./weblist/500_1000"
	  //val sourceFile = "./weblist/test.file"
	  val webList = sc.textFile(sourceFile)
	  val sortFunc = new OrderedRDDFunctions(webList.map(r => (r, 1)))
	  val sortedList = sortFunc.sortByKey()

	  //Read DNS records 
	  val original_data = sc.textFile("/data1/sie/ch202/201212/raw_processed.201212*.0.gz")
	  //val original_data = sc.textFile("/data1/sie/ch202/201212/raw_processed.20121201.0100.1354323600.064745979.0.gz")
	  //Parse answer
	  val data = original_data.map(x => new ParseDNSFast().convert(x))

	  /*
	  webList.foreach( oneDomain => {
	  //for(oneDomain <- webList.toArray){
	  	println("Domain: " + oneDomain)
	  	val hitRecords = data.filter(r => {
	  		val tmp = new parseUtils().parseDomain(r._5, oneDomain)
	  		//println(tmp)
	  		tmp.equalsIgnoreCase(oneDomain+".")
	  		})
	  	//val outFile = new java.io.FileWriter(outPath + oneDomain)
	  	val timestamp = hitRecords.map(r => r._1).toArray

	  	for(t <- timestamp){
	  		println("TIMESTAMP: " + t)
	  		val filename = new parseUtils().convertStampToFilename(t)
	  		val rdd1 = sc.textFile(dataPath + filename(0)).map(x => new ParseDNSFast().convert(x))
	  		val rdd2 = sc.textFile(dataPath + filename(1)).map(x => new ParseDNSFast().convert(x))
	  		var partial_data = rdd2.++(rdd1)
	  		if (filename.length == 3){
	  			
	  			val rdd3 = sc.textFile(dataPath + filename(2)).map(x => new ParseDNSFast().convert(x)) 
	  			partial_data = partial_data.++(rdd3)	
	  		}
	  		
	  		partial_data.filter(r => ((t - r._1) < 60 && (t - r._1) > 0)).filter(r => {
	  				val tmp = new parseUtils().parseDomain(r._5, oneDomain+".")
	  				val distance = new DLDistance().distance(tmp, oneDomain+".")
	  				distance <= 2 && distance > 0
	  				}).foreach(println)

/*
	  		for(name <- filename){
	  			val partial_data = sc.textFile(dataPath + name).map(x => new ParseDNSFast().convert(x))
	  			partial_data.filter(r => ((t - r._1) < 60 && (t - r._1) > 0)).filter(r => {
	  				val tmp = new parseUtils().parseDomain(r._5, oneDomain+".")
	  				val distance = new DLDistance().distance(tmp, oneDomain+".")
	  				distance <= 2 && distance > 0
	  				}).foreach(println)*/
	  					//r => {
	  					//val str = r._1.toString + "," + r._2.toString + "," + r._3 + "," +r._4 + "," + r._5 + "\n"
	  					//outFile.write(str)
	  					//})
	  		
	  	}
	  	//outFile.close

	  }*/

	  val partitioner = new HashPartitioner(webList.count.asInstanceOf[Int])
	  val arrWeb = sortedList.map(r => r._1).toArray
	  val data_pair = data.map(r => {
	  	/*var oneDomain = new scala.collection.mutable.StringBuilder()
	  	breakable{

	  		for( domain <- arrWeb) {
	  	 		val tmp = new parseUtils().parseDomain(r._5, domain+".")
	  	 		val distance = new DLDistance().distance(tmp, domain+".")
	  	 		if (distance <= 2){
	  	 			oneDomain.clear()
	  	 			oneDomain.append(domain)
	  	 			break
	  	 		}
	  	 	}
	  	 	oneDomain.clear()
	  	 	oneDomain.append("NOTHING")
	  	}*/
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
	  				file.createNewFile();
	  			}
	  			val fileWriter = new FileWriter(file.getName(),true);
	  			val bufferwriter = new BufferedWriter(fileWriter);
	  			
	  			//val outFile = new java.io.FileWriter(outPath + filename)
	  			r.map(record => new ParseDNSFast().antiConvert(record._2)).foreach(r => bufferwriter.write(r+"\n"))
	  			//outFile.close
	  			bufferwriter.close
	  		}
	  	})

	  /*
	  val oneDomain = webList.toArray.apply(0)
	  println(oneDomain)
	  val tmp = data.filter(r =>{
	  	val temp = new parseUtils().parseDomain(r._5, oneDomain+".")
	  	val distance = new DLDistance().distance(temp, oneDomain+".")
	  	distance <=2
	  	})

	  println("Count: " + tmp.count)
	  */
	}
}