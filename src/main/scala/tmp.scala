package org.edmond.tmp

import scala.io.Source
import scala.io._
import java.io.File
import scala.math._

class tmp{
	val keyboard = initialize();
	def initialize(): scala.collection.mutable.HashMap[Char, Array[Char]] = {
		val lines = scala.io.Source.fromFile("/Users/edmond/Typosquatting/src/main/scala/keyboard-dist").getLines.toList
		//val lines = scala.io.Source.fromFile("/home/xiang/Typosquatting/src/main/scala/keyboard-dist").getLines.toList
		val set = new scala.collection.mutable.HashMap[Char, Array[Char]]()
		for(line <- lines){
			val char = line.split(' ').apply(0).charAt(0)
			val tmpBuffer = scala.collection.mutable.ArrayBuffer[Char]()
			for (c <- line.split(' ')){
				if(c.charAt(0) != char){
					tmpBuffer.+=:(c.charAt(0))
				}
			}
			set.+=((char, tmpBuffer.toArray))
		}
		return set
	}

	def minimum(i1: Float, i2: Float, i3: Float)=min(min(i1, i2), i3)
	def distance(s1:String, s2:String)={
		val keyboard = initialize();
       	val dist=Array.tabulate(s1.length+1, s2.length+1){(j,i)=>if(j==0) i.toFloat else if (i==0) j.toFloat else 0.toFloat}

       	for(j<-1 to s2.length; i<-1 to s1.length){
        	var cost = if(s1(i-1) == s2(j-1)) 0.toFloat else 1.toFloat
        	val arr = keyboard(s1(i-1))

         	for(c <- arr){
         		if(c == s2(j-1)){
         			cost = cost - 0.5.toFloat
         		}
        	}
        	val deletion = dist(i-1)(j)+1
        	val insertion = dist(i)(j-1)+1
        	val substitution = dist(i-1)(j-1)+cost

        	dist(i)(j)=minimum(deletion, insertion, substitution)

        	if(i > 1 && j > 1 && s1(i-1)==s2(j-2) && s1(i-2)==s2(j-1)){
        		dist(i)(j) = min(dist(i)(j).toFloat,dist(i-2)(j-2).toFloat+cost)
        	}
       	}
       	dist(s1.length)(s2.length)
    }	
}