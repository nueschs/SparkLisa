package ch.unibnf.mcs.sparklisa.receiver

import akka.actor.Actor
import ch.unibnf.mcs.sparklisa.statistics.RandomTupleGenerator
import ch.unibnf.mcs.sparklisa.topology.NodeType
import org.apache.log4j.Logger
import org.apache.spark.streaming.receiver.ActorHelper

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Random

class NumericalRandomTupleReceiver(nodes: List[NodeType], rate: Double, numRandomValues: Int) extends Actor with ActorHelper {

  val random = new Random()
  private val sleepDuration: Int = ((60.0)/ rate).toInt
  val log = Logger.getLogger(getClass)
  val statsGen = RandomTupleGenerator

  override def preStart = {
    log.info(s"Sleep duration set to $sleepDuration")
    context.system.scheduler.schedule(5 seconds, sleepDuration seconds)({
      val values: mutable.MutableList[(Int, List[List[Int]])] = mutable.MutableList()

      for (node <- nodes) {
        val id : Int = node.getNodeId.substring(4).toInt
        values += ((id, statsGen.createRandomNeighboursListNumerical(node.getNodeId, numRandomValues, nodes.size)))
      }
      val size = values.size
      log.info(s"Sending $size values")
      self ! values.iterator
    })
  }

  case class SensorSimulator()

  override def receive = {
    case data: Iterator[(Int, List[List[Int]])] => {
      store[(Int, List[List[Int]])](data)
    }
  }

}
