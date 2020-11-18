package scala_homework

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.breakable

object Part3 {
  def main(args: Array[String]): Unit = {
    /**
     * UserA,LocationA,8,60
     * UserA,LocationA,9,60
     * UserB,LocationB,10,60
     * UserB,LocationB,11,80
     */
    var ls = ArrayBuffer(("UserA", "LocationA", 8, 60), ("UserA", "LocationA", 9, 60), ("UserB", "LocationB", 10, 60), ("UserB", "LocationB", 11, 80));
    var sortedList = ls.sortWith((t1, t2) => {
          if (t1._1.compareTo(t2._1) != 0) {
            t1._1.compareTo(t2._1) < 0
          } else if (t1._2.compareTo(t2._2) != 0) {
            t1._2.compareTo(t2._2) < 0
          } else if (t1._3 != t2._3) {
            t1._3 < t2._3
          }
          t1._4 < t2._4
        });

    val result = mergeList(sortedList);
    println(result);
    // Output: ArrayBuffer((UserA,LocationA,8,120), (UserB,LocationB,10,140))
  }

  //(name, location, time, duration)
  //algorithm: two pointer linear scan, time: O(n), space: O(1)
  def mergeList(ls: ArrayBuffer[(String, String, Int, Int)]): ArrayBuffer[(String, String, Int, Int)] = {
    val n = ls.length;
    var prev = ls.head;
    val res: ArrayBuffer[(String, String, Int, Int)] = ArrayBuffer[(String, String, Int, Int)]();

    for (i <- 1 until n) {
      val cur = ls(i);
      if (cur._1 != prev._1 || cur._2 != prev._2) {
        res.append(prev);
        prev = cur;
      } else {
        if (prev._3 * 60 + prev._4 >= cur._3 * 60) {
          prev = (prev._1, prev._2, prev._3, prev._4 + cur._4);
        } else {
          res.append(prev);
          prev = cur;
        }
      }
    }

    //append the last prev
    res.append(prev);
    return res;
  }
}
