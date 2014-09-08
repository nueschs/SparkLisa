package ch.unibnf.mcs.sparklisa.receiver

import akka.actor.Actor
import ch.unibnf.mcs.sparklisa.topology.Topology
import org.apache.spark.streaming.receiver.ActorHelper

import scala.collection.JavaConversions._
import scala.util.Random

class TopologySimulatorActorReceiver(topology: Topology, rate: Int) extends Actor with ActorHelper {

  private val random = new Random()
  private val sleepDuration : Int= (rate / 60)*1000

  override def preStart = {
    self ! SensorSimulator()
  }

  case class SensorSimulator()

  def receive = {
    case _: SensorSimulator =>{
      for (node <- topology.getNode){
        store[(String, Double)]((node.getNodeId, random.nextGaussian()))
      }
      Thread.sleep(sleepDuration)
      self ! SensorSimulator()
    }

  }
}