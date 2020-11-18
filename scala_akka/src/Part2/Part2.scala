package scala_homework.Part2

import scala.collection.mutable.ArrayBuffer
import scala.io.StdIn
import scala.util.Random
import scala.util.control.Breaks.{break, breakable}

object Part2 {
  val drawStr = "draw";
  val winStr = "You Win!";
  val loseStr = "You Lose!";

  def main(args: Array[String]): Unit = {
    println("---------------------Welcome-------------------\n------------------Rock, Paper, Scissors---------------\n----------------------------------");

    val roleMap = Map(1 -> "ZhangFei", 2 -> "GuanYu", 3 -> "LiuBei");
    val ls = ArrayBuffer[(String, Int, Int, Int)]();

    breakable {
      while (true) {
        println("new game! pick a role: 1, 2, 3.......")
        var c = StdIn.readChar();

        var oppRole = c - '0';
        printf("you picked to play against %s\n", roleMap.get(oppRole).get);

        println("do you want to start? y/n");
        c = StdIn.readChar();
        if (c != 'y') {
          break;
        }

        var myCount = 0;
        var oppCount = 0;

        breakable {
          while (true) {
            println("please play 1. sissor 2. rock 3. paper");
            c = StdIn.readChar();
            var play = c - '0';
            var oppPlay = Random.nextInt(3) + 1;
            if (play == oppPlay) {
              println(drawStr);
            } else if (play > oppPlay) {
              println(winStr);
              myCount += 1;
            } else {
              println(loseStr);
              oppCount += 1;
            }
            println("continue play? y/n");
            c = StdIn.readChar();
            if (c.equals('n')) {
              break;
            }
          }
        }

        var res: String = "";
        if (myCount == oppCount) {
          res = drawStr;
        } else if (myCount > oppCount) {
          res = winStr;
        } else {
          res = loseStr;
        }
        ls.append((res, oppRole, myCount, oppCount));
        printf("result: %s, do you want to continue for another game?", res);
        c = StdIn.readChar();
        if (c == 'n') {
          break;
        }
      }
    }

    println("Game result: ");
    ls.foreach(t => printf("Game result against %d, : %s, your score: %d, your opponent score: %d\n", t._2, t._1, t._3, t._4));
  }
}
