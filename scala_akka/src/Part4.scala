package homework41
import akka.actor.typed.scaladsl.LoggerOps
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
case class Message(from: ActorRef[Message], message: String);

object AActor {
  var counter = 0;
  var maxCounter = 3;
  def apply(): Behavior[Message] = Behaviors.receive {
      (context, message) =>
        this.counter += 1;
        if (this.counter > this.maxCounter) {
          Behaviors.stopped;
        } else {
          context.log.info("{}: sends message with content: {}", message.from, message.message);
          message.from ! Message(context.self, "AACtor message counter is currently: " + this.counter);
          Behaviors.same;
        }
    }
}

object BActor {
  var counter = 0;
  var maxCounter = 3;
  def apply(): Behavior[Message] = Behaviors.receive {
    (context, message) =>
      this.counter += 1;
      if (this.counter > this.maxCounter) {
        Behaviors.stopped;
      } else {
        context.log.info("{}: sends message with content: {}", message.from, message.message);
        message.from ! Message(context.self, "BACtor message counter is currently: " + this.counter);
        Behaviors.same;
      }
  }
}
object DriverActor {
  final case class StartMessage(message: String);

  def apply(): Behavior[StartMessage] = Behaviors.setup{
    context =>
      //spawn aActor and bActor
      val bActor = context.spawn(BActor(), "bActor");
      val aActor = context.spawn(AActor(), "aActor");

      //upon receiving message, initiate message sending between A and B
      Behaviors.receiveMessage {
        message =>
          bActor ! Message(aActor, "starting message!")
          Behaviors.stopped
      }
  }
}

object Part4 {
  def main(args: Array[String]): Unit = {
    val driver: ActorSystem[DriverActor.StartMessage] = ActorSystem(DriverActor(), "Part4Homework");
    driver ! DriverActor.StartMessage("start messaging!")

  }
}
