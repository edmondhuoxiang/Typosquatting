package org.edmond.webs

import org.edmond.DLDistance._
import org.edmond.utils._
import org.edmond.dnsproc_spark._
import org.apache.log4j.Logger
import org.apache.log4j.Level


import spark.SparkContext
import spark.SparkContext._

import scala.io.Source
import scala.io._
import java.io.File

object webs extends Application {
	override def main(args: Array[String]): Unit = {

	  Logger.getLogger("spark").setLevel(Level.INFO)

	  val sparkHome = "/Users/edmond/spark-0.7.3"
	  val jarFile = "target/scala-2.9.3/dnsudf_spark_2.9.3-0.0.jar"
	  val master = "local[20]"
	  val sc = new SparkContext(master, "dnsudf_spark", sparkHome, Seq(jarFile))

	  val outPath = "./res/"
	  val dataPath = "/data1/sie/ch202/201212/"

	  //Reading correct domain name from file
	  val sourceFile = "./weblist/500_1000"
	  val webList = sc.textFile(sourceFile).cache

	  //Read DNS records 
	  val original_data = sc.textFile("/data1/sie/ch202/201212/raw_processed.201212*.gz")

	  //Parse answer
	  val data = original_data.map(x => new ParseDNSFast().convert(x))

	  for(oneDomain <- webList.toArray){
	  	println("Domain: " + oneDomain)
	  	val hitRecords = data.filter(r => {
	  		val tmp = new parseUtils().parseDomain(r._5, oneDomain)
	  		//println(tmp)
	  		tmp.equalsIgnoreCase(oneDomain+".")
	  		})
	  	val outFile = new java.io.FileWriter(outPath + oneDomain)
	  	val timestamp = hitRecords.map(r => r._1).toArray

	  	for(t <- timestamp){
	  		println("TIMESTAMP: " + t)
	  		val filename = new parseUtils().convertStampToFilename(t)
	  		for(name <- filename){
	  			val partial_data = sc.textFile(dataPath + name).map(x => new ParseDNSFast().convert(x))
	  			partial_data.filter(r => ((t - r._1) < 60 && (t - r._1) > 0)).filter(r => {
	  				val tmp = new parseUtils().parseDomain(r._5, oneDomain)
	  				val distance = new DLDistance().distance(tmp, oneDomain)
	  				distance <= 2 && distance > 0
	  				}).foreach(r => {
	  					val str = r._1.toString + "," + r._2.toString + "," + r._3 + "," +r._4 + "," + r._5 + "\n"
	  					outFile.write(str)
	  					})
	  		}
	  	}
	  	outFile.close

	  }
	}
}