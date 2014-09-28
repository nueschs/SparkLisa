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

class TriggerableTemporalTopologySimulatorActorReceiver(nodes: List[NodeType], rate: Double, k: Int) extends Actor with ActorHelper {

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
  private val values: mutable.Map[Int, Queue[Double]] = mutable.Map()

  override def preStart = {
    context.actorOf(ReceiverActor.props())
  }

  override def receive = {
    case _ =>
  }

  def storeNewValueBatch() = {
    val pushValues: ArrayBuffer[(Int, Array[Double])] = ArrayBuffer()
    for (node <- nodes) {
      val nodeId: Int = node.getNodeId.substring(4).toInt
      if (!values.exists(_._1 == nodeId)){
        values.put(nodeId, Queue(random.nextGaussian()))
      } else {
        values(nodeId) = values(nodeId).enqueueFinite(random.nextGaussian(), k)
      }
      pushValues += ((nodeId, values(nodeId).toArray))
    }
    store(pushValues.iterator)
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