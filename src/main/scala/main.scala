package org.edmond.Main

import org.edmond.dnsproc_spark._
import org.apache.log4j.Logger
import org.apache.log4j.Level


import spark.SparkContext
import spark.SparkContext._

import scala.io.Source
import scala.io._
import java.io.File

object Main extends Application{

   def recursiveListFiles(f: File): Array[File] = {
   	val these = f.listFiles
   	these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
   }

   def printRecord(record: (Int, Int, String, String, String, Int, Int, List[Array[String]], List[Array[String]], List[Array[String]])) = {
      print(record._1 + ", " + record._2 + ", " + record._3 + ", " + record._4 + ", " + record._5 + ", " + record._6 + ", " + record._7 + ", ")
      print("{")
      for (arr <- record._8){
         print("||")
         for(a <- arr){
            print("(" + a + ")")
         }
      }
      print("}, {")
      for (arr <- record._9){
         print("||")
         for(a <- arr){
            print("(" + a + ")")
         }
      }
      print("}, {")
      for (arr <- record._10){
         print("||")
         for(a <- arr){
            print("(" + a + ")")
         }
      }
      print("}\n")
   }


   override def main(args: Array[String]): Unit = {
   	val outPath = "./res/"
   	Logger.getLogger("spark").setLevel(Level.INFO)

   	val sparkHome = "/Users/edmond/spark-0.7.3"
   	val jarFile = "target/scala-2.9.3/dnsudf_spark_2.9.3-0.0.jar"
   	val master = "local[20]"
   	val sc = new SparkContext(master, "dnsudf_spark", sparkHome, Seq(jarFile))

      val outFile = new java.io.FileWriter(outPath + "result")

      //Read records from dist
   	val original_data = sc.textFile("/data1/sie/ch202/201212/*.0.gz")
      //val original_data = sc.textFile("/data1/sie/ch202/201212/test_data")
   	
      //Parse answer
      val data = original_data.map(x => new ParseDNSFast().convert(x)).filter(l => l._3.startsWith("68.105"))
   	val countData = data.count()
   	println(countData + " records are parsed.")
      outFile.write(countData + " records are parsed.\n")

      // * Get the count of records which includes .ocm.
      val ocmRecords = data.filter(r => (r._5.endsWith(".ocm.")))
      val countOcmRecords = ocmRecords.count
      println(countOcmRecords + " records include \'.ocm.\' as a typo domain.")
      outFile.write(countOcmRecords + " records include \'.ocm.\' as a typo domain.\n")

      // * Whether or not the .com version of the domain in question would have been present in that resolver's cache at that time, assuming no cache eviction before TTL
      // * For .ocm lookups where the .com version was not cached at the same time, what is the % chance that you see a lookup (from the same resolver) for that domain name within 60 seconds.
      val ocmDomain = ocmRecords.map(record => (record._1, record._3, record._4, record._5)).toArray


      var resCached = new collection.mutable.ArrayBuffer[(Int, String, String, String, Long, String)]
      var resNotCached = new collection.mutable.ArrayBuffer[(Int, String, String, String, Long, String)]
      val timeThreshold = 60 //60 second
      for (line <- ocmDomain){
         val domain = line._4.replace(".ocm.",".com.")
         val resCount = data.filter(r => (r._5.equalsIgnoreCase(domain))).filter(r => !r._8.isEmpty).filter(r => ((r._8.apply(0).apply(1).toInt + r._1)< line._1) && (line._3 == r._3)).count
         val tmp = (line._1, line._2, line._3, line._4, resCount, if(resCount > 0) "YES" else "NO")
         resCached += tmp  

         //Not cached
         if (resCount == 0){
            val resCount2 = data.filter(r => (r._5.equalsIgnoreCase(domain))).filter(r => (r._1 > line._1) && (r._1 < line._1 + timeThreshold) && (line._3 == r._3)).count
            val tmp2 = (line._1, line._2, line._3, line._4, resCount2, if(resCount2 > 0) "YES" else "NO")
            resNotCached += tmp2
         }
      }

      val numCachedCom = resCached.filter(line => line._5 > 0).length
      val numCached = resNotCached.filter(line => line._5 > 0).length.toDouble
      val numNotCached = resNotCached.length
      sc.parallelize(resCached).saveAsTextFile(outPath+"CachedOrNot")
      sc.parallelize(resNotCached).saveAsTextFile(outPath+"notCached")
      println(numCachedCom + " ocm records that when they happen the correct versions are cached in solvers.")
      println("The % chance that you see a lookup (from the same resolver) for that domain name within "+ timeThreshold +" seconds is " + numCached/numNotCached + "(" + numCached + "\\" + numNotCached + ").")
      outFile.write(numCachedCom + " ocm records that when they happen the correct versions are cached in solvers.\n")
      outFile.write("The % chance that you see a lookup (from the same resolver) for that domain name within "+ timeThreshold +" seconds is " + numCached/numNotCached + "(" + numCached + "\\" + numNotCached + ").\n")

      outFile.close
   }
}
