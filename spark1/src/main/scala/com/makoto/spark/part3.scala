package com.makoto.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object part3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("part3").setMaster("local[*]")
    val spark = new SparkContext(conf)
    spark.setLogLevel("warn")

    val clicklogStr = "INFO 2019-09-01 00:29:53 requestURI:/click?app=1&p=1&adid=18005472&industry=469&adid=31\nINFO 2019-09-01 00:30:31 requestURI:/click?app=2&p=1&adid=18005474&industry=469&adid=31\nINFO 2019-09-01 00:31:03 requestURI:/click?app=1&p=1&adid=18005472&industry=469&adid=32\nINFO 2019-09-01 00:31:51 requestURI:/click?app=1&p=1&adid=18005472&industry=469&adid=33"
    val implogStr = "INFO 2019-09-01 00:29:53 requestURI:/imp?app=1&p=1&adid=18005472&industry=469&adid=31\nINFO 2019-09-01 00:29:53 requestURI:/imp?app=1&p=1&adid=18005473&industry=469&adid=34"

    val clicklogRdd: RDD[(String, Int)] = spark.parallelize(clicklogStr.split("\\n"), 2).map(line => (line.split("&")(2), 1));
    // clicklogRdd.collect().foreach(println)
    val clicklogAggRdd= clicklogRdd.reduceByKey(_ + _);
    clicklogAggRdd.foreach(println) // (adid=18005474,1), (adid=18005472,3)
    //implogStr.split("\\n").foreach(println)
    val implogRdd = spark.parallelize(implogStr.split("\\n", 2)).map(line => (line.split("&")(2), 1));
    val implogAggRdd= implogRdd.reduceByKey(_ + _);
    implogAggRdd.foreach(println) //(adid=18005473,1), (adid=18005472,1)

    //full outer join on clicklog and implog
    val outerJoin: RDD[(String, (Option[Int], Option[Int]))] = clicklogAggRdd.fullOuterJoin(implogAggRdd)

    val res = outerJoin.map{case(adid, (click, imp)) => (adid, ("click count = " + click.getOrElse(0), "imp count = " + imp.getOrElse(0)))};
    res.foreach(println)
    // (adid=18005472,(click count = 3,imp count = 1)), (adid=18005473,(click count = 0,imp count = 1)), (adid=18005474,(click count = 1,imp count = 0))
  }
}
