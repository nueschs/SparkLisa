package ch.unibnf.mcs.sparklisa.receiver

import akka.util.ByteString
import akka.actor.Actor
import org.apache.spark.streaming.receivers.Receiver
import akka.actor.IOManager
import akka.actor.IO
import scala.util.Random

class SensorSimulatorReceiver extends Actor with Receiver {

  private final val random = new Random();

  def receive = {
    case true => pushBlock(createValue())
  }

  private def createValue(): Double = {
    Thread.sleep(500L);
    return random.nextGaussian();
  }

}