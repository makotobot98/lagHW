package homework41
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}

import scala.collection.immutable.HashMap
import scala.concurrent.duration.{Duration, FiniteDuration, SECONDS}

sealed trait ClusterMessage;
final case class RegisterWorker(worker: ActorRef[ClusterMessage], reportTime: String) extends ClusterMessage;
final case class ReportHeartBeat(worker: ActorRef[ClusterMessage], reportTime: String) extends ClusterMessage;
final case class ConfirmRegister() extends ClusterMessage;
final case class StartCluster() extends ClusterMessage;
final case class Timeout() extends ClusterMessage;
object Worker {
  val TIMEOUT_DURATION: FiniteDuration = FiniteDuration(Duration("2 seconds").toSeconds, SECONDS);
  val formatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private case class Setup(id: Int, masterRef: ActorRef[ClusterMessage] ,timer: TimerScheduler[ClusterMessage], context: ActorContext[ClusterMessage]);

  def apply(id: Int, masterRef: ActorRef[ClusterMessage]): Behavior[ClusterMessage] = {
    Behaviors.setup { context =>
      Behaviors.withTimers { timer =>
        worker(Setup(id, masterRef, timer, context));
      }
    }
  }

  def worker(setup: Setup): Behavior[ClusterMessage] = {
    register(setup);
  }

  def register(setup: Setup): Behavior[ClusterMessage] = {
    val curDate: Date = Calendar.getInstance().getTime;
    val curTime: String = formatter.format(curDate);
    println(s"worker ${setup.id} registering self to the cluster .....")
    setup.masterRef ! RegisterWorker(setup.context.self, curTime);
    Behaviors.receiveMessage { message =>
      message match {
        case ConfirmRegister() => {
          println(s"worker ${setup.id}'s registration confirmed by the master .....")
          idle(setup);
        }
        case _ => Behaviors.same
      }
    }
  }

  def idle(setup: Setup): Behavior[ClusterMessage] = {
    setup.timer.startSingleTimer(Timeout(), this.TIMEOUT_DURATION);
    reportHeartBeat(setup)
  }

  def reportHeartBeat(setup: Setup): Behavior[ClusterMessage] = {
    Behaviors.receiveMessage { message =>
      message match {
        case Timeout() => {
          val curDate: Date = Calendar.getInstance().getTime;
          val curTime: String = formatter.format(curDate);
          println(s"worker ${setup.id} reporting heartbeat at time ${curTime} .....");
          ReportHeartBeat(setup.context.self, curTime)
          idle(setup)
        }
        case _ => Behaviors.same
      }
    }
  }
}

object Master {
  val formatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  val MASTER_TIMEOUT_DURATION: FiniteDuration = FiniteDuration(Duration("3 seconds").toSeconds, SECONDS);
  val MAX_ALLOWED_WORKER_TIMEOUT_SEC: Int = 5;

  private case class Setup(timer: TimerScheduler[ClusterMessage], context: ActorContext[ClusterMessage], map: HashMap[ActorRef[ClusterMessage], String]);
  def apply(): Behavior[ClusterMessage] = {
    Behaviors.setup { context =>
      Behaviors.withTimers { timer =>
        master(Setup(timer, context, HashMap.empty));
      }
    }
  }

  def master(setup: Setup): Behavior[ClusterMessage] = {
    idle(setup)
  }

  def idle(setup: Setup): Behavior[ClusterMessage] = {

    setup.timer.startSingleTimer(Timeout(), MASTER_TIMEOUT_DURATION);
    active(setup);
  }

  def active(setup: Setup): Behavior[ClusterMessage] = {
    Behaviors.receiveMessage{ message => {
      message match {
        case Timeout() => {                           //check alive children
          println("master updating worker map .....")
          val curMilli: Long = Calendar.getInstance().getTimeInMillis;
          val calendar: Calendar = Calendar.getInstance();
          //filter out the outdated child
          val newMap = setup.map.filter(entry => isAlive(curMilli, formatter.parse(entry._2).getTime));
          idle(Setup(setup.timer, setup.context, newMap));  //back to idle state
        }
        case ReportHeartBeat(worker, reportTime) => { //update heartbeat
          println(s"master receives heart beat report from worker ${worker.toString} at time ${reportTime}");
          val newMap = setup.map.map(entry => {
            if (entry._1 == worker) {
              (entry._1, reportTime);
            } else {
              (entry._1, entry._2)
            }
          });
          println(s"........updated worker map: ${newMap.toString}");
          active(Setup(setup.timer, setup.context, newMap));
        }
        case RegisterWorker(worker, reportTime) => {  //update worker list
          println(s"master receives register request from worker ${worker.toString} at time ${reportTime}");
          val newMap = setup.map ++ HashMap(worker -> reportTime);
          worker ! ConfirmRegister();
          println(s"........updated worker map: ${newMap.toString}");
          active(Setup(setup.timer, setup.context, newMap));
        }
        case _ => {                                   //unknown
          Behaviors.same;
        }
      }
    }}
  }
  def isAlive(curMilli: Long, pastTime: Long ): Boolean = {
    (curMilli - pastTime).abs < this.MAX_ALLOWED_WORKER_TIMEOUT_SEC
  }

}

object ClusterDriver {
  var autoIncrementId: Int = 1;
  def apply(): Behavior[ClusterMessage] = {

    Behaviors.setup{context =>
      println("initializing the cluster ....");
      Behaviors.receiveMessage { message =>
        //spawn master
        println("Driver: spawning master....")
        val master = context.spawn(Master(), "cluster_master");

        //spawn worker
        println("Driver: spawning worker 1....")
        val w1 = context.spawn(Worker(1, master), "worker_1");
        Thread.sleep(1000);
        println("Driver: spawning worker 2....")
        Thread.sleep(1000);
        val w2 = context.spawn(Worker(2, master), "worker_2");
        /*
        Thread.sleep(1000);
        println("Driver: spawning worker 3....")
        val w3 = context.spawn(Worker(3, master), "worker_3");
         */
        Thread.sleep(10000);
        Behaviors.stopped;
      }
    }
  }
}
object Part5 {
  def main(args: Array[String]): Unit = {
    val system: ActorSystem[ClusterMessage] = ActorSystem(ClusterDriver(), "MasterWorkerCluster");
    system ! StartCluster();
  }
}
