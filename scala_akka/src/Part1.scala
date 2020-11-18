package scala_homework

object Part1 {
  def solution(nlid: Int, nbottle: Int, total: Int): Int = {
    if (nlid < 5 && nbottle < 3) {
      return total;
    }

    var cur = nlid / 5 + nbottle / 3;
    return solution(nlid % 5 + cur, nbottle % 3 + cur, total + cur);
  }
  def main(args: Array[String]): Unit = {
    val res = 50 + solution(50, 50, 0);
    println(res); //104
  }
}
