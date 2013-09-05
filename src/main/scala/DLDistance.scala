package org.chris.dnsproc
import org.apache.hadoop.filecache.DistributedCache
import scala.math._

/* Implementation of pseudocode found at
http://en.wikipedia.org/wiki/Damerau%E2%80%93Levenshtein_distance
for Damerau-Levenschtein edit distance.
*/

class DLDistance {
   /*
  override def outputSchema(input:Schema) : Schema = {
        try {
            val fieldSchema = new Schema.FieldSchema("distance", DataType.INTEGER)
            return new Schema(fieldSchema)
        }
        catch {
            case e: Exception => {
                return null
            }
        }
    }  
  */
  /*
  def exec(input: Tuple): Int = {
        val d1 = input.get(0).asInstanceOf[String]
        val d2 = input.get(1).asInstanceOf[String]
        try {
            return distance(d1,d2)
        }
        catch {
            case e: Exception => {
                println("failed to parse: " + e )
                return -1
            }
        }
    }
*/
    def minimum(i1: Int, i2: Int, i3: Int)=min(min(i1, i2), i3)
    def distance(s1:String, s2:String)={
       val dist=Array.tabulate(s1.length+1, s2.length+1){(j,i)=>if(j==0) i else if (i==0) j else 0}

       for(j<-1 to s2.length; i<-1 to s1.length){
         val cost = if(s1(i-1) == s2(j-1)) 0 else 1

         dist(i)(j)=minimum(dist(i-1)(j)+1,dist(i)(j-1)+1,dist(i-1)(j-1)+cost)

         if(i > 1 && j > 1 && s1(i-1)==s2(j-2) && s1(i-2)==s2(j-1)){
            dist(i)(j) = min(dist(i)(j),dist(i-2)(j-2)+cost)
          }
       }

       dist(s1.length)(s2.length)
    }

    def insert(originStr: String, pos: Int): Array[String] ={
      val insertArray = new scala.collection.mutable.ArrayBuffer[String]()
      val insertStr = new scala.collection.mutable.StringBuilder()
      for (i <- 0 until 26){
        insertStr.append(originStr)
        val char = 'a'+i
        insertStr.insert(pos, char.asInstanceOf[Char])
        insertArray += insertStr.toString
        insertStr.clear()
      }
      return insertArray.toArray
    } 

    def del(originStr: String, pos: Int): String ={
      val deletionStr = new scala.collection.mutable.StringBuilder()
      deletionStr.append(originStr)
      deletionStr.deleteCharAt(pos)
      return deletionStr.toString
    }   

    def subs(originStr: String, pos: Int): Array[String]= {
      val substitutionArray = new scala.collection.mutable.ArrayBuffer[String]()
      val substitutionStr = new scala.collection.mutable.StringBuilder()
      for(i <- 0 until 26){
        substitutionStr.append(originStr)
        val char = 'a' + i
        substitutionStr.deleteCharAt(pos)
        substitutionStr.insert(pos, char.asInstanceOf[Char])
        substitutionArray += substitutionStr.toString
        substitutionStr.clear()
      }
      return substitutionArray.toArray
    }

    def trans(originStr: String): Array[String] = {
      val transArray = new scala.collection.mutable.ArrayBuffer[String]()
      val transStr = new scala.collection.mutable.StringBuilder()
      for(i <- 0 until originStr.length-1){
        transStr.append(originStr)
        val char = originStr.charAt(i)
        transStr.deleteCharAt(i)
        transStr.insert(i+1, char)
        transArray += transStr.toString
        transStr.clear()
      }
      return transArray.toArray
    }

    def typoCandidate(originStr: String): Array[String] = {
      val result = new scala.collection.mutable.ArrayBuffer[String]()
      
      //all possible typo with insertion
      for(i <- 0 until originStr.length+1){
        val arr = insert(originStr, i)
        result.appendAll(arr)
      }

      //all possible typo with deletion
      for(i <- 0 until originStr.length){
        val str = del(originStr, i)
        result.append(str)
      }

      //all possible typo with substitution
      for(i <- 0 until originStr.length){
        val arr = subs(originStr, i)
        result.appendAll(arr)
      }
      //all possible type with transposition
      val arr = trans(originStr)
      result.appendAll(arr)
    return result.distinct.toArray
  }
}

object DLDistance {

  //val test_values = "once" :: "upon" :: "A" :: "time" :: "test" :: "tset" :: "tsett" :: Nil
    val test_values = new DLDistance().typoCandidate("testvalues")
    def main(args: Array[String]): Unit = {
      for(t <- test_values) {
        println(t + " " + new DLDistance().distance(t,"testvalues"))
      }
    }
}
