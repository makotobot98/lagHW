package com.makoto.sparkStream

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD


object GraphX {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("graphX").setMaster("local[*]")
    val spark = new SparkContext(conf)
    spark.setLogLevel("warn")

    val varr = Array((1L, "SFO"), (2L, "ORD"), (3L, "DFW"))
    val earr = Array(Edge(1L, 2L, 1800), Edge(2L, 3L, 800) , Edge(3L, 1L, 1400))
    val vrdd = spark.makeRDD(varr)
    val erdd = spark.makeRDD(earr)
    val graph = Graph(vrdd, erdd)

    //Q1
    graph.vertices.foreach(println)
    //Q2
    graph.edges.foreach(println)
    //Q3
    graph.triplets.foreach(println)
    //Q4
    val vertexCnt = graph.vertices.count()
    println("Q4" + vertexCnt)
    //Q5
    val edgeCnt = graph.edges.count()
    println("Q5" + edgeCnt)

    //Q6
    graph.edges.filter(_.attr > 1000).foreach(println)
    //Q7
    graph.edges.sortBy(-_.attr).foreach(println)

  }
}
