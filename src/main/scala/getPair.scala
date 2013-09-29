package org.edmond.webs

import org.apache.log4j.Logger
import org.apache.log4j.Level
import spark.SparkContext
import spark.SparkContext._
import org.edmond.utils._
import org.edmond.dnsproc_spark._
import org.edmond.DLDistance._

import scala.io.Source
import scala.io._
import java.io.File
import java.io.FileWriter
import java.io.BufferedWriter
import java.io.IOException

object getPair extends Serializable {

	def getDomain(path: String): String = {
		val index = path.lastIndexOf('/')
		return path.substring(index+1, path.length)
	}

	def main(args: Array[String]): Unit = {
		System.setProperty("spark.default.parallelism","500")
	  	Logger.getLogger("spark").setLevel(Level.INFO)

	  	val sparkHome = "/Users/edmond/spark-0.7.3"
	  	val jarFile = "target/scala-2.9.3/dnsudf_spark_2.9.3-0.0.jar"
	  	val master = "local[20]"

	  	val outPath = "./Pair/"
	  	val dataPath = "./webfiles"

	  	//get source files list
	  	val dir = new File(dataPath)
	
	  	val files = new ListFiles().recursiveListFiles(dir)
	  	val numPerTime = 1

	  	var index = 0
	  	val sc = new SparkContext(master, "dnsudf_spark", sparkHome, Seq(jarFile))
/*
	  	val domain = sc.parallelize(files)

	  	domain.foreach(r => {
	  		val data = sc.textFile(r.toString)
	  		println(data.count)
	  		})*/
	  	while(index < files.length){

	  		
	  		var num=0
	  		if(index+numPerTime < files.length){
	  			num = numPerTime
	  		}
	  		else{
	  			num = files.length - index;
	  		}

	  		println("Processing files from " +(index+1)+ " to " + (index+num));
	  		val data = sc.textFile(files.apply(index).toString).map(x => new ParseDNSFast().convert(x))
	  		val domain = getDomain(files.apply(index).toString)
	  		val correctRcd = data.filter(r=>{
	  			val rcdDomain = new parseUtils().parseDomain(r._5, domain+".")
	  			val dis = new DLDistance().distance(rcdDomain, domain+".") 
	  			dis == 0
	  			}).toArray

	  		correctRcd.foreach(r => {
	  			val time = r._1
	  			data.filter(rcd => ((rcd._1 <= time) && ((time - rcd._1) < 60))).foreach(r => println(r._1 + " " +r._5))
	  			})
	  		

	  		index+=num
	  	}
	  	
	}
	
}