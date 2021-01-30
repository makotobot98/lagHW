package com.makoto.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.math.{pow, sqrt, max}

object part5 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("part5 k means").setMaster("local[*]")
    val spark = new SparkContext(conf)
    spark.setLogLevel("warn")

    val NITER = 30; //max # of iterations
    val NCENTROID = 3;
    val MINDIST: Double = 0.1;

    val X: RDD[(Double, Double, Double, Double)] = spark.textFile("data/Iris.txt").map(line => {
      val split = line.split("\t");
      (split(1).toDouble, split(2).toDouble, split(3).toDouble, split(4).toDouble)
    })


    //initialize centroid to random points
    val centroids: Array[(Double, Double, Double, Double)] = X.takeSample(false, NCENTROID);

    var iter = 0;
    var minDist = Double.MaxValue;
    while (iter < NITER && minDist > MINDIST) {
      println(s"iteration ${iter}, minDist = ${minDist}");
      val assignmentRdd: RDD[(Int, Iterable[(Double, Double, Double, Double)])] = X.map(x => {
        val d1 = pow(x._1 - centroids(0)._1, 2) + pow(x._2 - centroids(0)._2, 2) + pow(x._3 - centroids(0)._3, 2) + pow(x._4 - centroids(0)._4, 2);
        val d2 = pow(x._1 - centroids(1)._1, 2) + pow(x._2 - centroids(1)._2, 2) + pow(x._3 - centroids(1)._3, 2) + pow(x._4 - centroids(1)._4, 2);
        val d3 = pow(x._1 - centroids(2)._1, 2) + pow(x._2 - centroids(2)._2, 2) + pow(x._3 - centroids(2)._3, 2) + pow(x._4 - centroids(2)._4, 2);

        val d = Seq(d1, d2, d3);
        val dIndex: Seq[(Double, Int)] = d.zipWithIndex;
        //assignment of x to centroids
        val assignment = dIndex.min._2;
        (assignment, x);
      }).groupByKey();

      //compute new centroid
      val updateCentroidRdd: RDD[(Int, (Double, Double, Double, Double))] = assignmentRdd.mapValues(arr => {
        val ls = arr.toList;
        //compute average
        val sum = ls.reduce((t1, t2) => {
          (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3, t1._4 + t2._4);
        });
        (sum._1 / ls.size, sum._2 / ls.size, sum._3 / ls.size, sum._4 / ls.size);
      });

      val res: collection.Map[Int, (Double, Double, Double, Double)] = updateCentroidRdd.collectAsMap();




      //update iter and min distance
      iter = iter + 1;
      val dist1 = dist(centroids(0), res(0));
      val dist2 = dist(centroids(1), res(1));
      val dist3 = dist(centroids(2), res(2));
      minDist = max(max(dist1, dist2), dist3)

      //update centroid
      centroids(0) = res(0);
      centroids(1) = res(1);
      centroids(2) = res(2);
    }

    //print result
    centroids.foreach(println);
    //(4.772,2.968000000000001,1.744,0.3360000000000001)
    //(5.234615384615385,3.68076923076923,1.4769230769230768,0.28076923076923077)
    //(6.3182795698924705,2.888172043010753,4.9655913978494635,1.6924731182795703)

  }
  def dist(t1: (Double, Double, Double, Double), t2: (Double, Double, Double, Double)) = {
    sqrt(pow(t1._1 - t2._1, 2) + pow(t1._2 - t2._2, 2) + pow(t1._3 - t2._3, 2) + pow(t1._4 - t2._4, 2))
  }
}
