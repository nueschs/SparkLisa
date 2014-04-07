package ch.unibnf.mcs.sparklisa.receiver

import akka.util.ByteString
import akka.actor.Actor
import org.apache.spark.streaming.receivers.Receiver
import akka.actor.IOManager
import akka.actor.IO
import scala.util.Random

class SensorSimulatorActorReceiver extends Actor with Receiver {

  private final val random = new Random();

  override def preStart = init()

  case class SensonSimulator

  def receive = {
    case _: SensonSimulator => pushBlock(createValue())
  }

  private def init() = {
    Thread.sleep(500L);
    self ! SensonSimulator()
  }

  private def createValue(): Double = {
    Thread.sleep(50L);
    self ! SensonSimulator()
    return random.nextGaussian();
  }

}