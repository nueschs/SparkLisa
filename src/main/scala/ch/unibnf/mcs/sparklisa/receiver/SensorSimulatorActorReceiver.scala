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
import ch.unibnf.mcs.sparklisa.sensor_topology.Topology
import ch.unibnf.mcs.sparklisa.sensor_topology.NodeType
import ch.unibnf.mcs.sparklisa.sensor_topology.BasestationType
import scala.collection.JavaConversions._

class SensorSimulatorActorReceiver extends Actor with Receiver {

  private final var topology: Topology = readXml()
  private final val random = new Random()

  override def preStart = init()

  case class SensonSimulator

  def receive = {
    case _: SensonSimulator => pushNodeBlocks()
  }

  def pushNodeBlocks() = {
    Thread.sleep(500L);

    for (station <- topology.getBasestations().getBasestation()) {
      for (xmlNode <- station.getManagedNodes().getNode()) {
        var node: NodeType = xmlNode.getValue().asInstanceOf[NodeType]
        pushBlock((node, random.nextGaussian()))
      }
    }

    self ! SensonSimulator()
  }

  private def init() = {
    Thread.sleep(500L);
    self ! SensonSimulator()
  }

  private def readXml(): Topology = {
    var is = getClass().getClassLoader().getResourceAsStream("xml/simple_topology.xml")
    var context = JAXBContext.newInstance(classOf[Topology])
    var unmarshaller = context.createUnmarshaller()
    var t: Topology = unmarshaller.unmarshal(is).asInstanceOf[Topology]
    return t;
  }

}