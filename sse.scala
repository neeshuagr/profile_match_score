package spark2.parallelProcess
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.HashPartitioner
import scala.xml.XML

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.util.control._
import scala.util.control.Breaks._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import java.util.Calendar

import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import scala.util.Try

object gParser {

  def main(args: Array[String]) {

    // val sparkSession = SparkSession.builder.master("local").appName("sse").getOrCreate()
    // val df = sparkSession.read.option("header","true").csv("/home/neeshu/Downloads/dump.csv")
    // df.printSchema()
    // df.show()
    // val sc = new sparkSession(sparkSession)
    // val graphDocs = sc.parallelize(Documents(sc), 200).map(x=>(x._1, x._2)).persist(StorageLevel.MEMORY_ONLY)


    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.scheduler.mode", "FAIR")
    conf.set("spark.localExecution.enabled", "true")
    conf.set("spark.eventLog.enabled", "true")
    // conf.set("spark.akka.frameSize", "300")
    
    val sc = new SparkContext(conf)
    val graphDocs = sc.parallelize(Documents(sc), 200).map(x=>(x._1, x._2)).persist(StorageLevel.MEMORY_ONLY)

    val edge_file = sc.textFile("/home/ec2-user/mnt/nlpdata/int_edges.txt")
    println("The count of int_edges file : "+edge_file.count)
    val semantic_edges = edge_file.map(line => {val x = line.split("\\s+")
                                                (x(0).toInt, (x(1).toInt, x(2).toInt, x(3).toInt))})


    for (i <- 1 to 5) {
                println("Press Enter for the next search.")
                val search_doc = sc.textFile("/home/ec2-user/mnt/nlpdata/search.txt")
                println("The count of search file : "+search_doc.count)
                val search_doc_edges =  search_doc.flatMap(line => {val x = line.split("\\s+")
                                                                    Array(x(0).toLong, x(1).toLong) }).collect().toList.distinct
                search_doc_edges.foreach(println)

                val intersection = semantic_edges.filter(x => search_doc_edges.contains(x._2._1) || search_doc_edges.contains(x._2._2))
                val inter_ints = intersection.map(x => (x._1, x._2._3))
                val inter_int_count = inter_ints.count
                val normalizer = inter_ints.values.reduce(_+_).toFloat / inter_int_count.toFloat
                val start_time = Calendar.getInstance().getTime()
                val joined = graphDocs.join(inter_ints).map(x => (x._2._1, x._2._2)).mapValues(x => (x, 1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2)).mapValues(x => x._1.toFloat / (x._2.toFloat * normalizer))
                val result = joined.collect
                println("Joined Graph Count :" +  result.length)
                println(start_time + " : " + Calendar.getInstance().getTime())
                println("Press Enter to search. Normalizer Index: " + normalizer + " Search sub graph edge count: " + inter_int_count)
                readLine()
                joined.collect.foreach(println)
            }

    
    readLine()
    sc.stop()
    System.exit(0)

  }

  def Documents(sc: SparkContext) = {
        val document_file = Source.fromFile("/home/ec2-user/mnt/nlpdata/integer_documents_edges.txt", "UTF-8")
        val lineIterator = document_file.getLines
        var docLineCount = 0
        var jdLineCount = 0
        var docId = ""
        val edges = ArrayBuffer[String]()
        val graphCollection = new ArrayBuffer[(Int, Int)]()

        for (l <- lineIterator) {

            if (docLineCount != 0 && l.contains("-")){
                docLineCount = 0
                graphCollection ++= CreateGraph(docId, edges, sc, jdLineCount)

                edges.clear()
                // println(docId)
            }

            if (l.contains("-")){
               docId = l
               jdLineCount += 1
               // print('.')

               if (jdLineCount % 1000 == 0){
                    print(jdLineCount + " ") // + " " + graphRDD.count.toString)
               }
            }
            else
                edges += l
            docLineCount += 1
        }
        // println(jdLineCount)
        graphCollection
        // graphRDD
    }

    def CreateGraph(graphId:String, edges: ArrayBuffer[String], sc:SparkContext, jobid: Int) = {
        val intEdges = edges.map(m => {
            val e = m.split(" ")
            (e(0).toInt, jobid)
        })
        intEdges
    }

    
}