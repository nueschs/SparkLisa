package ch.unibnf.mcs.sparklisa.receiver

import akka.util.ByteString
import akka.actor.Actor
import org.apache.spark.streaming.receivers.Receiver
import akka.actor.IOManager
import akka.actor.IO
import scala.util.Random
import java.io.InputStream
import ch.unibnf.mcs.sparklisa.xml.XmlParser
import scala.io.Source._
import javax.xml.bind.JAXBContext
import scala.collection.JavaConversions._
import ch.unibnf.mcs.sparklisa.topology.NodeType

class SensorSimulatorActorReceiver(node: NodeType) extends Actor with Receiver {

  private final var sensorNode: NodeType = node

  private final val random = new Random()

  override def preStart = init()

  case class SensorSimulator

  def receive = {
    case _: SensorSimulator => pushNodeBlocks()
  }

  def pushNodeBlocks() = {
    Thread.sleep(500L);
    pushBlock((sensorNode, random.nextGaussian()))
    self ! SensorSimulator()
  }

  private def init() = {
    Thread.sleep(500L);
    self ! SensorSimulator()
  }

}