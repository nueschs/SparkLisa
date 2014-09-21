package ch.unibnf.mcs.sparklisa.receiver

import akka.actor.Actor
import ch.unibnf.mcs.sparklisa.topology.NodeType
import org.apache.spark.streaming.receiver.ActorHelper

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Random
import scala.collection.immutable.Queue

class TimeBasedTopologySimulatorActorReceiver(nodes: List[NodeType], rate: Double, k: Int) extends Actor with ActorHelper {

  class FiniteQueue[A](q: Queue[A]) {
    def enqueueFinite[B >: A](elem: B, maxSize: Int): Queue[B] = {
      var ret = q.enqueue(elem)
      while (ret.size > maxSize) { ret = ret.dequeue._2 }
      ret
    }
  }
  implicit def queue2finitequeue[A](q: Queue[A]) = new FiniteQueue[A](q)

  private val random = new Random()
  private val sleepDuration: Int = (60.0 / rate).toInt
  private val values: mutable.Map[Int, Queue[Double]] = mutable.Map()

  override def preStart = {
    context.system.scheduler.schedule(10 seconds, sleepDuration seconds)({
      val pushValues: ArrayBuffer[(Int, Array[Double])] = ArrayBuffer()
      for (node <- nodes) {
        val nodeId: Int = node.getNodeId.substring(4).toInt
        if (!values.exists(_._1 == nodeId)){
          values.put(nodeId, Queue(random.nextGaussian()))
        } else {
          values(nodeId) = values(nodeId).enqueueFinite(random.nextGaussian(), k)
        }
        pushValues += ((node.getNodeId.substring(4).toInt, values(nodeId).toArray))
      }
      self ! pushValues.iterator
    })
  }

  case class SensorSimulator()

  override def receive = {
    case data: Iterator[(Int, Array[Double])] => {
      store[(Int, Array[Double])](data)
    }
  }


}