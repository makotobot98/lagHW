package scala_homework.Part2

import scala.io.StdIn

class User(name: String) extends Player(name) {
  def play() = {
    StdIn.readInt();
  }
}
