package scala_homework.Part2

import scala.util.Random

class Computer(name: String) extends Player(name) {
  def play() = Random.nextInt(3) + 1;
}
