package ch.unibnf.mcs.sparklisa.receiver

import akka.actor.Actor
import ch.unibnf.mcs.sparklisa.topology.NodeType
import org.apache.log4j.Logger
import org.apache.spark.streaming.receiver.ActorHelper

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Random

/**
 * Creates a random value (gaussian distributed Double values) for each node at each batch interval
 * @param nodes
 * @param rate
 */
class NumericalTopologySimulatorActorReceiver(nodes: List[NodeType], rate: Double) extends Actor with ActorHelper {

  val random = new Random()
  private val sleepDuration: Int = ((60.0)/ rate).toInt
  val log = Logger.getLogger(getClass)

  override def preStart = {
    log.info(s"Sleep duration set to $sleepDuration")
    context.system.scheduler.schedule(5 seconds, sleepDuration seconds)({
      val values: mutable.MutableList[(Int, Double)] = mutable.MutableList()
      for (node <- nodes) {
          val id: Int = node.getNodeId.substring(4).toInt
          values += ((id, random.nextGaussian()))
      }
      val size = values.size
      log.info(s"Sending $size values")
      self ! values.iterator
    })
  }

  case class SensorSimulator()

  override def receive = {
    case data: Iterator[(Int, Double)] => {
      store[(Int, Double)](data)
    }
  }
}