package com.makoto.spark

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object part2 {
  val formatter: SimpleDateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
  val outFormatter: SimpleDateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH");
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("part2").setMaster("local[*]")
    val spark = new SparkContext(conf)
    spark.setLogLevel("warn")

    val cdnStr = "100.79.121.48 HIT 33 [15/Feb/2017:00:00:46 +0800] \"GET http://cdn.v.abc.com.cn/videojs/video.js HTTP/1.1\" 200 174055 \"http://www.abc.com.cn/\" \"Mozilla/4.0+(compatible;+MSIE+6.0;+Windows+NT+5.1;+Trident/4.0;)\"\n111.19.97.15 HIT 18 [15/Feb/2017:00:00:39 +0800] \"GET http://cdn.v.abc.com.cn/videojs/video-js.css HTTP/1.1\" 200 14727 \"http://www.zzqbsm.com/\" \"Mozilla/5.0+(Linux;+Android+5.1;+vivo+X6Plus+D+Build/LMY47I)+AppleWebKit/537.36+(KHTML,+like+Gecko)+Version/4.0+Chrome/35.0.1916.138+Mobile+Safari/537.36+T7/7.4+baiduboxapp/8.2.5+(Baidu;+P1+5.1)\"\n218.108.100.234 HIT 1 [15/Feb/2017:00:00:57 +0800] \"GET http://cdn.v.abc.com.cn/videojs/video.js HTTP/1.1\" 200 174050 \"http://www.abc.com.cn/\" \"Mozilla/5.0+(Windows+NT+6.1;+WOW64)+AppleWebKit/537.36+(KHTML,+like+Gecko)+Chrome/53.0.2785.116+Safari/537.36\"\n125.105.41.123 HIT 6734 [15/Feb/2017:00:07:03 +0800] \"GET http://cdn.v.abc.com.cn/140987.mp4 HTTP/1.1\" 206 12785299 \"http://m.8531.cn/news/556707.html?from=timeline&isappinstalled=0&weixin_share_count=4\" \"Mozilla/5.0+(Linux;+U;+Android+4.4.4;+zh-cn;+OPPO+R7+Build/KTU84P)+AppleWebKit/533.1+(KHTML,+like+Gecko)+Mobile+Safari/533.1\"\n211.138.116.41 HIT 2523 [15/Feb/2017:00:08:17 +0800] \"GET http://cdn.v.abc.com.cn/140987.mp4 HTTP/1.1\" 206 12785299 \"-\" \"AppleCoreMedia/1.0.0.14D27+(iPhone;+U;+CPU+OS+10_2_1+like+Mac+OS+X;+zh_cn)\"";
    cdnStr.split("\\n").foreach(println)
    val cdnRdd: RDD[Array[String]] = spark.parallelize(cdnStr.split("\\n"), 2).map(_.split(" "));

    //2.1 group by ip with index offset = 0
    val individualIPRdd = cdnRdd.groupBy(_(0));
    val individualIPSize = individualIPRdd.collect().size;
    println(individualIPSize); //print 5

    //2.2 compute individual ip count by video, video offset = 6
    val individualIPByVidRdd: RDD[(String, Int)] = cdnRdd.filter(_(6).endsWith(".mp4")).map(arr => (arr(6), 1))
    val individualIPCount: collection.Map[String, Long] = individualIPByVidRdd.countByKey();
    individualIPCount.foreach(println); //print (http://cdn.v.abc.com.cn/140987.mp4,2)

    //2.3 compute individual ip count by hour of a day, time offset = 3
    val ipByHourRdd = cdnRdd.map(arr => (parseTime(arr(3).substring(1)), 1));
    val ipByHourCount = ipByHourRdd.countByKey();
    ipByHourCount.foreach(println); //print (15/Feb/2017:00,5)
  }
  def parseTime(s: String): String = {
    val date: Date = formatter.parse(s);
    outFormatter.format(date);
  }
}
