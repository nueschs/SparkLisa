package ch.unibnf.mcs.sparklisa.receiver

import java.net.InetSocketAddress

import akka.actor.{Props, Actor}
import akka.io.Tcp.{Register, Connected, Bind}
import akka.io.{IO, Tcp}
import ch.unibnf.mcs.sparklisa.topology.NodeType
import org.apache.log4j.Logger
import org.apache.spark.streaming.receiver.ActorHelper
import scala.collection.mutable

import scala.util.Random

class TriggerableTopologySimulatorActorReceiver(nodes: List[NodeType], rate: Double) extends Actor with ActorHelper {

  val random = new Random()
  val log = Logger.getLogger(getClass)

  override def preStart = {
    context.actorOf(ReceiverActor.props())
  }

  override def receive = {
    case _ =>
  }

  def storeNewValueBatch() = {
    val values: mutable.MutableList[(String, Double)] = mutable.MutableList()
    for (node <- nodes) {
      values += ((node.getNodeId, random.nextGaussian()))
    }
    store(values.iterator)
  }

  object ReceiverActor {
    def props() = {
      Props(new ReceiverActor)
    }
  }

  class ReceiverActor extends Actor with Serializable {

    import Tcp._
    import context.system

    IO(Tcp) ! Bind(self, new InetSocketAddress(23456))

    override def receive = {
      case c @ Connected(remote, local) => {
        val handler = context.actorOf(Props(new ReceiverHandler))
        val connection = sender
        connection ! Register(handler)
      }
    }
  }

  class ReceiverHandler extends Actor with Serializable{
    import akka.io.Tcp._
    def receive = {
      case Received(data) => {
        log.info(">>> received data, creating new batch")
        storeNewValueBatch()
      }
    }
  }

}