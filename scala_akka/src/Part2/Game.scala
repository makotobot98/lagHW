package scala_homework.Part2

import scala.io.StdIn
import scala.util.control.Breaks.{break, breakable}

class Game {
  var user: User = null;
  var computer: Computer = null;
  var gameCount = 0;
  var userScoreCnt = 0;
  var computerScoreCnt = 0;
  var drawCnt = 0;
  val roleMap = Map(1 -> "ZhangFei", 2 -> "GuanYu", 3 -> "LiuBei");
  val getPCNameStr = "new game! pick a role: 1 ZhangFei, 2 GuanYu, 3 LiuBei......."
  val getPlayStr = "please play 1. sissor 2. rock 3. paper";
  val drawStr = "draw";
  val winStr = "You Win!";
  val loseStr = "You Lose!";

  def init() = {
    user = new User("user");

  }
  def start() = {
    breakable {
      while (true) {
        gameCount += 1;
        println(getPCNameStr);
        var c = getInput();
        computer = new Computer(roleMap.get(c - '0').get);
        printf("you picked to play against %s\n", computer.name);
        println("do you want to start? y/n");
        c = getInput();
        if (c != 'y') {
          break;
        }
        user.resetScore();
        computer.resetScore();

        breakable {
          while (true) {
            println(getPlayStr);
            c = getInput();
            var play = c - '0';
            var oppPlay = computer.play();

            if (play == oppPlay) {
              println(drawStr);
            } else if (play > oppPlay) {
              println(winStr);
              user.addScore();
            } else {
              println(loseStr);
              computer.addScore();
            }

            println("continue play? y/n");
            c = getInput();
            if (c.equals('n')) {
              break;
            }
          }
        }

        var res: String = "";
        if (user.score == computer.score) {
          drawCnt += 1;
          res = drawStr;
        } else if (user.score > computer.score) {
          userScoreCnt += 1;
          res = winStr;
        } else {
          computerScoreCnt += 1;
          res = loseStr;
        }

        printf("result: %s, do you want to continue for another game? y/n\n", res);
        c = getInput();
        if (c == 'n') {
          break
        }
      }
    }

    println("Game is finished");
    printf("Game Count: %d, \nName Draw Win Lose\nYou  %d   %d   %d \nPC    %d   %d    %d\n", gameCount, drawCnt, userScoreCnt, computerScoreCnt, drawCnt, computerScoreCnt, userScoreCnt);
  }
  def getInput(): Char = StdIn.readChar();
}
