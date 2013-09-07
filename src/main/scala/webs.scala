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

	  //Reading correct domain name from file
	  val sourceFile = "./weblist/500_1000"
	  val weblist = sc.textFile(sourceFile).cache

	  //Read DNS records 
	  val original_data = sc.textFile("/data1/sie/ch202/201212/*.0.gz")

	  //Parse answer
	  val data = original_data.map(x => new ParseDNSFast().convert(x))

	  val oneDomain = weblist.take(1).apply(0)
	  //val oneDomain = "google.com"
	  println("Domain: " + oneDomain)
	  val hitRecords = data.filter(r => {
	  	val tmp = new parseUtils().parseDomain(r._5, oneDomain)
	  	//println(tmp)
	  	tmp.equalsIgnoreCase(oneDomain+".")
	  	})
	  val timestamp = hitRecords.map(r => r._1).toArray
	  for(t <- timestamp){
	  	data.filter(r => ((t - r._1) < 60 && (t - r._1) > 0)).filter(r => {
	  		val tmp = new parseUtils().parseDomain(r._5, oneDomain)
	  		val distance = new DLDistance().distance(tmp, oneDomain)
	  		distance <= 2
	  		}).foreach(println)
	  }
	}
}