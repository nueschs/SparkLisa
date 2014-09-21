package ch.unibnf.mcs.sparklisa.receiver

import java.net.InetSocketAddress

import akka.actor.{Actor, Props}
import akka.io.{IO, Tcp}
import ch.unibnf.mcs.sparklisa.topology.NodeType
import org.apache.log4j.Logger
import org.apache.spark.streaming.receiver.ActorHelper

import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class TriggerableTimeBasedTopologySimulatorActorReceiver(nodes: List[NodeType], rate: Double, k: Int) extends Actor with ActorHelper {

  class FiniteQueue[A](q: Queue[A]) {
    def enqueueFinite[B >: A](elem: B, maxSize: Int): Queue[B] = {
      var ret = q.enqueue(elem)
      while (ret.size > maxSize) { ret = ret.dequeue._2 }
      ret
    }
  }
  implicit def queue2finitequeue[A](q: Queue[A]) = new FiniteQueue[A](q)

  val random = new Random()
  val log = Logger.getLogger(getClass)
  private val values: mutable.Map[String, Queue[Double]] = mutable.Map()

  override def preStart = {
    context.actorOf(ReceiverActor.props())
  }

  override def receive = {
    case _ =>
  }

  def storeNewValueBatch() = {
    val pushValues: ArrayBuffer[(String, Array[Double])] = ArrayBuffer()
    for (node <- nodes) {
      if (!values.exists(_._1 == node.getNodeId)){
        values.put(node.getNodeId, Queue(random.nextGaussian()))
      } else {
        values(node.getNodeId) = values(node.getNodeId).enqueueFinite(random.nextGaussian(), k)
      }
      pushValues += ((node.getNodeId, values(node.getNodeId).toArray))
    }
    store(values.iterator)
  }

  object ReceiverActor {
    def props() = {
      Props(new ReceiverActor)
    }
  }

  class ReceiverActor extends Actor with Serializable {

    import akka.io.Tcp._
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