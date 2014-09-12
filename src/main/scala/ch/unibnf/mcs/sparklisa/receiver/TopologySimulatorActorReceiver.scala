package ch.unibnf.mcs.sparklisa.receiver

import akka.actor.{ActorSystem, UntypedActor, Actor}
import ch.unibnf.mcs.sparklisa.topology.{NodeType, Topology}
import org.apache.spark.streaming.receiver.ActorHelper

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.util.Random
import scala.concurrent.ExecutionContext.Implicits.global

class TopologySimulatorActorReceiver(nodes: List[NodeType], rate: Int) extends Actor with ActorHelper {

  private val random = new Random()
  private val sleepDuration: Int = (rate / 60) * 1000
  val system = ActorSystem("Receiver")

  override def preStart = {
    context.system.scheduler.schedule(500 milliseconds, 1000 milliseconds)({
      for (node <- nodes) {
        self !(node.getNodeId,
          random.nextGaussian())
      }
    })
  }

  case class SensorSimulator()

  override def receive = {
    case data: (String, Double) => {
      store[(String, Double)](data)
    }
  }
}