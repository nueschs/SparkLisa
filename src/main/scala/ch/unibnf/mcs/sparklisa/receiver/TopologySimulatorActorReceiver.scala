package ch.unibnf.mcs.sparklisa.receiver

import akka.actor.Actor
import scala.util.Random
import javax.xml.bind.JAXBContext
import scala.collection.JavaConversions._
import ch.unibnf.mcs.sparklisa.topology.Topology
import ch.unibnf.mcs.sparklisa.topology.NodeType
import org.apache.spark.streaming.receiver.ActorHelper

class TopologySimulatorActorReceiver extends Actor with ActorHelper {

  private final var topology: Topology = readXml()
  private final val random = new Random()

  override def preStart = init()

  case class SensonSimulator()

  def receive = {
    case _: SensonSimulator => pushNodeBlocks()
  }

  def pushNodeBlocks() = {
    Thread.sleep(500L);

    for (station <- topology.getBasestation()) {
      for (xmlNode <- station.getNode()) {
        var node: NodeType = xmlNode
        store((node, random.nextGaussian()))
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