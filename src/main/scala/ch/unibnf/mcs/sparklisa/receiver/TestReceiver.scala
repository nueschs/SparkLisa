package ch.unibnf.mcs.sparklisa.receiver

import akka.actor.Actor
import org.apache.spark.streaming.receiver.ActorHelper

import scala.util.Random

class TestReceiver(nodes: List[String]) extends Actor with ActorHelper {

  case class TestSimulator()

  val random = new Random()

  override def preStart = {
    self ! TestSimulator()
  }

  def receive = {

    case _: TestSimulator => {
      for (node <- nodes) {
        store[(String, Double)]((node, random.nextGaussian()))
      }
    }
    Thread.sleep(500L)
    self ! TestSimulator()
  }

}
