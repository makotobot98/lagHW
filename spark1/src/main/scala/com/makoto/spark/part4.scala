package com.makoto.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable
import scala.math.pow

object part4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("part4 knn").setMaster("local[*]")
    val spark = new SparkContext(conf)
    spark.setLogLevel("warn")

    //training data
    val dataTr = "5.1,3.5,1.4,0.2,Iris-setosa\n4.9,3,1.4,0.2,Iris-setosa\n7,3.2,4.7,1.4,Iris-versicolor\n6.4,3.2,4.5,1.5,Iris-versicolor\n6.3,3.3,6,2.5,Iris-virginica\n6.3,2.5,5,1.9,Iris-virginica";

    val dataTrRdd: RDD[(Double, Double, Double, Double, String)] = spark.parallelize(dataTr.split("\\n"), 2).map(line => {
      val split = line.split(",");
      (split(0).toDouble, split(1).toDouble, split(2).toDouble, split(3).toDouble, split(4))
    });

    //test data
    val dataTe = "5,2.3,3.3,1\n7.3,2.9,6.3,1.8\n5,3.5,1.3,0.3";
    val dataTeRdd: RDD[(Double, Double, Double, Double)] = spark.parallelize(dataTe.split("\\n"), 2)map(line => {
      val split = line.split(",");
      (split(0).toDouble, split(1).toDouble, split(2).toDouble, split(3).toDouble)
    });


    //model prediction

    // 2 = # of neighbor

    val n = 2;

    val predictions = dataTeRdd.cartesian(dataTrRdd)
      .groupByKey()
      .map{case(k,v) => {
        //topK: list of k closest points to each k
        val topK: immutable.Seq[(String, Double)] = v.toList
          .map(x => {
            val distance = pow(k._1 - x._1, 2) + pow(k._2 - x._2, 2) + pow(k._3 - x._3, 2) + pow(k._4 - x._4, 2);
            (x._5, distance)
          })
          .sortBy(x => {
            x._2
          })(Ordering[Double].reverse)
          .take(n);

        val majority: (Int, String) = topK.groupBy(_._1).map{case(k,v) => (v.size, k)}.max;
        (k, majority._2);
      }}

    predictions.foreach(println);

    //output vs input:
    //input: (5.0,3.5,1.3,0.3,Iris-virginica), (7.3,2.9,6.3,1.8,Iris-setosa) (5.0,2.3,3.3,1.0,Iris-versicolor)
    //output: ((5.0,3.5,1.3,0.3),Iris-virginica), ((7.3,2.9,6.3,1.8),Iris-setosa), ((5.0,2.3,3.3,1.0),Iris-versicolor)
  }
}
