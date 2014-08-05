package ch.unibnf.mcs.sparklisa.receiver

import akka.actor.Actor
import org.apache.spark.streaming.receiver.ActorHelper

import scala.util.Random

/**
 * Created by snoooze on 04.08.14.
 */
class TestReceiver extends Actor with ActorHelper {

  case class TestSimulator()

  override def preStart = {
    self ! TestSimulator()
  }

  def receive = {
    case _: TestSimulator => store[Double](new Random().nextGaussian())
    Thread.sleep(500L)
    self ! TestSimulator()
  }

}
