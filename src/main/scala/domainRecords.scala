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

object domainRecords extends Serializable {
	def getDomainRecords(sc: SparkContext, inFile: String, outDir: String, domainArr: Array[String]) = {
		/*
		 * get all records about domain from input files and save them in outFile. "about" means
		 * every candidate domain queried by users and have edit distance less than 3 with domain.
		 */
		println("inFile: "+inFile)
		println("outDir: "+outDir)
		/*
		val domainRdd = sc.parallelize(domainArr, 20)
		val dnsRecords = sc.textFile(inFile).map(x => {
			new ParseDNSFast().convert(x)
		}).toArray.toList
*/
///////////////////////////////
/*		val domainRdd = sc.parallelize(domainArr, 20)
		val dnsRecords = sc.textFile(inFile, 20).map(x => {
			new ParseDNSFast().convert(x)
		}).toArray.toList

		domainRdd.foreach(domain => {
			val domainWithDot = domain+"."
			val resList = dnsRecords.filter(r => {
			val tmpDomain = new parseUtils().parseDomain(r._5, domainWithDot)
				val dist = new DLDistance().distance(tmpDomain, domain)
				dist >= 0 && dist <=2
			})

			if(resList.length != 0){
	  				val filename = domain
	  				println("Filename: " + filename)
	  				val file = new File(outDir + filename)
	  				//if file doesn't exists, then create it
	  				if(!file.exists()){
	  					file.createNewFile()
	  				}
	  				val fileWriter = new FileWriter(file.getAbsoluteFile(),true);
	  				val bufferwriter = new BufferedWriter(fileWriter);
	  			
	  				//val outFile = new java.io.FileWriter(outPath + filename)
	  				//r.map(record => new ParseDNSFast().antiConvert(record._2)).foreach(r => bufferwriter.write(r+"\n"))
	  				resList.foreach(r => {
	 	 				val line = new ParseDNSFast().antiConvert(r)
	  					bufferwriter.write(r+"\n")
	  				})
	  				//outFile.close
	  				bufferwriter.close
	  		}
		})	
*/

///////////////////////////////		
		val dnsRecords = sc.textFile(inFile, 20).map(x => {
			new ParseDNSFast().convert(x)
		})

		val pairRecords = dnsRecords.map(r => {
			val index = new parseUtils().lookUpString(r._5, domainArr, 0, domainArr.length-1)
			if(index > -1){
				val domain = domainArr.apply(index)
				(index, r)
			}
			else{
				(-1, r)	
			}
		}).filter(r => r._1 != -1).cache
		//}).filter(r => {!r._1.contentEquals("NOTHING")}).cache

		val partitioner = new HashPartitioner(domainArr.length.asInstanceOf[Int])
		var func = new PairRDDFunctions(pairRecords)
	  	val data_partitions = func.partitionBy(partitioner)

	  	data_partitions.foreachPartition(r => {
	  		if(r.nonEmpty){
	  			//val filename = r.take(1).toArray.apply(0)._1
	  			val index = r.take(1).toArray.apply(0)._1
	  			val filename = domainArr.apply(index)
	  			println("Filename: " + filename)
	  			val file = new File(outDir + filename)
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
		
	}


	def check(sc: SparkContext, inFile: String, outFile: String) = {
		val filename = inFile.split('/').toList.last
		val domain = filename+"."

		val dnsRecords = sc.textFile(inFile).map(x => new ParseDNSFast().convert(x))

		val distinct_domain = dnsRecords.map(r => {
			val tmp = new parseUtils().parseDomain(r._5, domain)
			tmp
		}).distinct

		val res = distinct_domain.filter(d => {
			val dist = new DLDistance().distance(d, domain)
			dist > 2
		}).toArray
		if(res.length > 0){
			val fileWriter = new FileWriter(outFile,true);
	  		val bufferwriter = new BufferedWriter(fileWriter);
	  		bufferwriter.write(filename + " ")
	  		for(d <- res){
	  			bufferwriter.write(d + " ")
	  		}
	  		bufferwriter.write("\n")
	  		bufferwriter.close
		}
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
	  	val master = "local[500]"
	  	val sc = new SparkContext(master, "dnsudf_spark", sparkHome, Seq(jarFile))


	  	val outPath = "./res/"
	  	val dataPath = "/data1/sie/ch202/201212/"
	  	val webListFile = "./weblist/500_1000"

	  	val webList = sc.textFile(webListFile)
////////////////////////	  	
	/*  	val sortFunc = new OrderedRDDFunctions(webList.map(r => (r, 1)))
	  	val sortedList = sortFunc.sortByKey()
	  	val arrWeb = sortedList.map(r => r._1).toArray

	  	getDomainRecords(sc, args.apply(0), outPath+args.apply(1), arrWeb)
*/
////////////////////////



		check(sc, "./res/sortedWebFiles/"+args.apply(0), outPath+"checkres.txt")
	}
}