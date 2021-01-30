package com.makoto.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object part1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("part1").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")

    val httpStr: String = "20090121000132095572000|125.213.100.123|show.51.com|/shoplist.php?phpfile=shoplist2.php&style=1&sex=137|Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1; Mozilla/4.0(Compatible Mozilla/4.0(Compatible-EmbeddedWB 14.59 http://bsalsa.com/EmbeddedWB- 14.59 from: http://bsalsa.com/ )|http://show.51.com/main.php|\n20090121000132095572000|132.213.100.123|show.51.com|/shoplist.php?phpfile=shoplist2.php&style=1&sex=137|Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1; Mozilla/4.0(Compatible Mozilla/4.0(Compatible-EmbeddedWB 14.59 http://bsalsa.com/EmbeddedWB- 14.59 from: http://bsalsa.com/ )|http://show.51.com/main.php|"
    val ipStr: String = "122.228.96.0|126.228.96.255|2061787136|2061787391|亚洲|中国|浙江|温州||电信|330300|China|CN|120.672111|28.000575\n130.0.0.0|139.999.999.999|2061787136|2061787391|亚洲|中国|云南|昆明||电信|330300|China|CN|120.672111|28.000575"

    //取出http.log中的ip地址并进行去重
    val httpRdd: RDD[String] = sc.parallelize(httpStr.split("\\n"), 2).map(line => line.split("\\|")(1)).distinct()
    //讲http log中ip地址的转换为Long以便于比较大小
    val httpLongRdd: RDD[Long] = httpRdd.map(convertIP(_))

    //取出ip.log起始ip范围以及对应城市
    // httpLongRdd.collect().foreach(println) >>125213100123, 132213100123 ...
    val ipRdd: RDD[(Long, Long, String)] = sc.parallelize(ipStr.split("\\n"), 2).map(line => {
      val split: Array[String] = line.split("\\|");
      //0 = start ip, 1 = end ip, 7 = city name
      (convertIP(split(0)), convertIP(split(1)), split(7));
    })

    // ipRdd.collect().foreach(println) >> (122228096000,12622896255,温州), (130000000000,139999999999,昆明), ...

    //cartisian product
    val ipInRangeRdd: RDD[(Long, (Long, Long, String))] = httpLongRdd.cartesian(ipRdd).filter(record => record._1 >= record._2._1 && record._1 <= record._2._2);
    val filterIPRdd = ipInRangeRdd.map{case(ip, (_, _, city)) => (city, ip)};
    // filterIPRdd.collect().foreach(println); >> (温州, 125213100123), (昆明, 132213100123)

    //根据城市聚集
    val aggByCityRdd = filterIPRdd.groupByKey().map{case(city, ips) => (city, ips.size)};

    //show result
    aggByCityRdd.collect().foreach(println);
    // (昆明,1), (温州,1), ...
  }
  def convertIP(ipStr: String): Long = {
    val split = ipStr.split("\\.");
    (split(0).toLong * math.pow(10, 9).toLong) + (split(1).toLong * math.pow(10, 6).toLong) + (split(2).toLong * math.pow(10, 3).toLong) + split(3).toLong
  }
}
