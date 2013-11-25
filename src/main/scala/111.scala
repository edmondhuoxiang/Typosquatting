  import scala.io.Source
  import scala.util.Random
  import spark.SparkContext
  import SparkContext._
  
  object Main{
  	
  
  val hostname = Source.fromFile("/root/mesos-ec2/masters").mkString.trim
  //computes the Euclidean distance between vectors a1 and a2
  def distL2(a1: Array[Double], a2: Array[Double]): Double = {
    require(a1.size == a2.size)
    var sum: Double = 0.0
    for(i <- a1.indices) sum += math.pow(a1(i) - a2(i), 2)
    math.sqrt(sum)
  }
  //class for computing running averages of real vectors
  class ArrAvg extends Serializable {
    private var avg: Array[Double] = null
    private var count: Int = 0      //number of vectors averaged thus far
    //constructor initializing this ArrAvg to be the average of the single vector arr
    def this(arr: Array[Double]) { this(); this += arr }
    //incorporate arr into this running average
    def +=(arr: Array[Double]): ArrAvg = {
      if(avg == null) avg = arr.clone    //if this average is currently empty
      else {
        //update avg to include arr
        require(avg.size == arr.size)
        for(i <- avg.indices) avg(i) = avg(i)*count/(count+1).toDouble + arr(i)/(count+1).toDouble
      }
      count += 1
      this
    }
    //incorporate all vectors averaged in other into this running average
    def ++=(other: ArrAvg): ArrAvg = {
      if(other.count == 0) return this   //if other is empty
      else if(avg == null) {
        //if this average is currently empty, set it equal to other
        avg = other.avg.clone
        count = other.count
      }
      else {
        //update this.avg and this.count based on the contents of other
        require(avg.size == other.avg.size)
        for(i <- avg.indices) {
          avg(i) *= count.toDouble/(count+other.count)
          avg(i) += other.avg(i)*other.count/(count+other.count).toDouble
        }
        count += other.count
      }
      this
    }
    def getAvg = avg
  }
  def clustering(sc: SparkContext) {
    //define settings
    //desired number of clusters
    val K = 10
    //stopping criterion for K-means iterations (threshold on average centroid change)
    val eps = 1e-6
    //load and cache the featurized data
    val rdd = sc.sequenceFile[String, String]("hdfs://"+hostname+":9000/wikistats_featurized").map{t => {
      //parse the string representation of the feature vectors used for data storage
      t._1 -> t._2.split(",").map(_.toDouble)
    }}.cache()
    //select a random subset of the data points as initial centroids
    var centroids = rdd.sample(false, 0.005, 23789).map(_._2).collect.map(_ -> Random.nextDouble).sortBy(_._2).map(_._1).take(K)
    println("Done selecting initial centroids")
    //run the K-means iterative procedure to find centroids
    var iterI = 0
    var oldCentroids = centroids
    var delta = 0.0
    //iterate until average centroid change (measured by Euclidean distance) is <= eps
    do {
      //store the previous set of centroids
      oldCentroids = centroids
      //compute new centroids; the aggregate() function on RDDs is analogous to
      //  the aggregate() function on Iterables
      centroids = rdd.map(_._2).aggregate(Array.fill(K)(new ArrAvg))((aa: Array[ArrAvg], arr: Array[Double]) => {
        //determine the centroid closest to feature vector arr
        val centroidI = centroids.indices.minBy{i => distL2(centroids(i), arr)}
        //incorporate arr into the ArrAvg corresponding to this closest centroid
        aa(centroidI) += arr
        aa
      }, (aa1: Array[ArrAvg], aa2: Array[ArrAvg]) => {
        //combine ArrAvgs computed for each new centroid
        (aa1 zip aa2).map(t => t._1 ++= t._2)
      }).map(_.getAvg)
      /* NOTE: if no points are assigned to a given centroid, then _.getAvg above will
               yield null; we do not currently handle this case, as this is only an
               exercise, and this situation is unlikely to occur on the given data;
               however, a proper implementation should handle this case */
      //compute the average change between elements of oldCentroids and new centroids
      delta = (centroids zip oldCentroids).map{t => distL2(t._1, t._2)}.reduce(_+_)/K.toDouble
      println("Finished iteration "+iterI+" (delta="+delta+")")
      iterI += 1
    } while(delta > eps)
    //print results
    println("Centroids with some articles:")
    val numArticles = 10
    for((centroid, centroidI) <- centroids.zipWithIndex) {
      //print centroid
      println(centroid mkString ("[",",","]"))
      //print numArticles articles which are assigned to this centroid’s cluster
      rdd.filter{t => {
        centroids.indices.minBy{i => distL2(centroids(i), t._2)} == centroidI
      }}.take(numArticles).foreach(println)
      println()
    }
  }

  def main(args: Array[String]): Unit = {
    	val sparkHome = "/Users/edmond/spark-0.7.3"
	  	val jarFile = "target/scala-2.9.2/scala-app-template_2.9.2-0.0.jar"
	  	val master = "local[20]"
		val sc = new SparkContext(hostname, "scala-app-template", sparkHome, Seq(jarFile))

		clustering(sc)
  }
  
}