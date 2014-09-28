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

class TestReceiver(rate: Double) extends Actor with ActorHelper {

  private val sleepDuration: Int = ((60.0)/ rate).toInt
  val log = Logger.getLogger(getClass)
  val statsGen = RandomTupleGenerator

  override def preStart = {
    log.info(s"Sleep duration set to $sleepDuration")
    context.system.scheduler.schedule(5 seconds, sleepDuration seconds)({
      val values: mutable.MutableList[Int] = mutable.MutableList()

      for (i <- 0 until 100000) {
        values += i
      }
      self ! values.iterator
    })
  }

  case class SensorSimulator()

  override def receive = {
    case data: Iterator[Int] => {
      store[Int](data)
    }
  }

}
