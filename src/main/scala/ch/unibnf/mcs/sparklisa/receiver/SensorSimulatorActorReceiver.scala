package ch.unibnf.mcs.sparklisa.receiver

import akka.actor.Actor
import scala.util.Random
import ch.unibnf.mcs.sparklisa.topology.NodeType
import scala.io.Source
import org.apache.spark.streaming.receiver.ActorHelper

class SensorSimulatorActorReceiver(node: NodeType) extends Actor with ActorHelper {

  private final val sensorNode: NodeType = node

  private final val random = new Random()

  //  val FILE_NAME = "/node_values_4.txt"
  val FILE_NAME = "/node_values_16_100_0.txt"

  var values: Array[Double] = Array[Double]();

  private var count: Int = 0

  override def preStart = init()

  case class SensorSimulator()


  def receive = {
    case _: SensorSimulator => pushNodeBlocks()
  }

  def pushNodeBlocks() = {
//    if (count < values.length) {
//      store[(String, Double)]((sensorNode.getNodeId, values(count)))
//      self ! SensorSimulator()
//      this.count += 1
//      Thread.sleep(500L)
//    }
    store[(String, Double)]((sensorNode.getNodeId, random.nextGaussian()))
    Thread.sleep(500L)
    self ! SensorSimulator()
  }

  private def init() = {
    //    Thread.sleep(500L);
    val pos: Int = Integer.parseInt(sensorNode.getNodeId.replace("node", "")) - 1
    val text = Source.fromInputStream(getClass().getResourceAsStream(FILE_NAME)).mkString
    readValues(text, pos)
    self ! SensorSimulator()
  }

  private def readValues(text: String, pos: Int) {
    val textArr = text.split("\n")
    val doubleStrings = textArr.map { l =>
      l.split(";")(pos)
    }
    val doubleValues = doubleStrings.map { s => s.toDouble}
    values = doubleValues
  }

}