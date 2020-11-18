package scala_homework.Part2

abstract class Player(var name: String) {
  var score: Int = 0;
  def addScore() = this.score += 1;
  def resetScore() = this.score = 0;
  def play(): Int;


}
