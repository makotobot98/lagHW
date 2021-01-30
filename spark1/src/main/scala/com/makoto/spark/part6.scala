package com.makoto.spark
import org.apache.spark.sql.{Row, SparkSession}

object part6 {
  def main(args: Array[String]): Unit = {


    val ss = SparkSession
      .builder()
      .appName("part 6")
      .master("local[*]")
      .getOrCreate()

    ss.sparkContext.setLogLevel("warn")

    import ss.implicits._
    val df = Seq((1, "2019-03-04", "2020-02-03"),(2, "2020-04-05", "2020-08-04"),(3, "2019-10-09", "2020-06-11")).toDF();

    val trimDF = df.select("_2").union(df.select("_3")).withColumnRenamed("_2", "date");

    trimDF.createOrReplaceTempView("t");

    ss.sql("select date, max(date) over (order by date rows between current row and 1 following) as d2 from t").show()

    //output:
    //+----------+----------+
    //|      date|        d2|
    //+----------+----------+
    //|2019-03-04|2019-10-09|
    //|2019-10-09|2020-02-03|
    //|2020-02-03|2020-04-05|
    //|2020-04-05|2020-06-11|
    //|2020-06-11|2020-08-04|
    //|2020-08-04|2020-08-04|
    //+----------+----------+

  }
}
