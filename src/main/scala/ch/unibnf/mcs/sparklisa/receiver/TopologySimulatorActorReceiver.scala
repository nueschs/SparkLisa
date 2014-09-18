package ch.unibnf.mcs.sparklisa.receiver

import akka.actor.{ActorSystem, UntypedActor, Actor}
import ch.unibnf.mcs.sparklisa.topology.{NodeType, Topology}
import org.apache.log4j.Logger
import org.apache.spark.streaming.receiver.ActorHelper

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.Random
import scala.concurrent.ExecutionContext.Implicits.global

class TopologySimulatorActorReceiver(nodes: List[NodeType], rate: Double) extends Actor with ActorHelper {

  val random = new Random()
  private val sleepDuration: Int = ((60.0)/ rate).toInt
  val log = Logger.getLogger(getClass)

  override def preStart = {
    log.info(s"Sleep duration set to $sleepDuration")
    context.system.scheduler.schedule(5 seconds, sleepDuration seconds)({
      val values: mutable.MutableList[(String, Double)] = mutable.MutableList()
      for (node <- nodes) {
          values += ((node.getNodeId, random.nextGaussian()))
      }
      val size = values.size
      log.info(s"Sending $size values")
      self ! values.iterator
    })
  }

  case class SensorSimulator()

  override def receive = {
    case data: Iterator[(String, Double)] => {
      store[(String, Double)](data)
    }
  }
}