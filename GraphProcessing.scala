package com.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import scala.io.Source
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.PairRDDFunctions
import java.lang
import java.util.Date

object GraphProcessing {
    def main(args: Array[String]) 
    {   val noOfIterations=args(0)
	      val conf = new SparkConf().setAppName("WikiGraph").setMaster("local[*]")      // creating the spark configuration
        val sContext = new SparkContext(conf)                                         // creating the spark context
        // Creating an RDD for String Vertex Name to Long Vertex
        val stringToLongMapping = sContext.textFile("/vyas/BigData/Assignment2/stringlongfolder").map
        {
	         inputLine => val data = inputLine.split("\\s+")                            // Splits the inputline according to spaces
	        (data(0).toString(),data(1).toString())                                     // Reversing the mapping for the join function
	         
       }.cache()
        
        // Load the Adjacency list from the file
	      val wikigraph = GraphLoader.edgeListFile(sContext, "/vyas/BigData/Assignment2/longvertex").cache()
        // Run PageRank with the convergence value
	      var time = new Date().getTime
	      val pageRanks = wikigraph.staticPageRank(noOfIterations.toInt).vertices.map
        //val pageRanks = wikigraph.pageRank(noOfIterations.toFloat).vertices.map
        {
	        case(vertex,rank) => (vertex.toString(),rank)
	      }.cache()
	      var time2 = new Date().getTime
	      
        //pageRanks.saveAsTextFile("/vyas/BigData/Assignment2/PageRank")               // Saving the PageRank into a file
	      // Join the String to Long mapping RDD and Result RDD to obtain the final page rank in the form <Vertex Name(Long), Rank>
	      val vertexRank = pageRanks.join(stringToLongMapping).map 
	      {
 		        case (id, (rank, vertex)) => (rank,vertex)
	      }
	      vertexRank.saveAsTextFile("/vyas/BigData/Assignment2/PageRank") 
        // Extracting the Top 100 Ranked vertex and saving an arraylist
        val topVertices = vertexRank.sortBy({row => row._1},false).top(100)
        
        // Saving the final result file into HDFS
        sContext.parallelize(topVertices).saveAsTextFile("/vyas/BigData/Assignment2/finalOutput")
        System.out.println("time taken:"+(time2-time))
        
}
}

