package ch.unibnf.mcs.sparklisa.receiver

import akka.actor.{Actor, ActorSystem}
import ch.unibnf.mcs.sparklisa.topology.NodeType
import org.apache.spark.streaming.receiver.ActorHelper

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Random

class SparkStreamingReceiver extends Actor with ActorHelper {

  private val random = new Random()

  override def preStart = {
    context.system.scheduler.schedule(500 milliseconds, 1000 milliseconds)({
        self ! ("string", random.nextGaussian())
    })
  }

  override def receive = {
    case data: (String, Double) => {
      store[(String, Double)](data)
    }
  }
}