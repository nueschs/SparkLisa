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

class TimeBasedTopologySimulatorActorReceiver(nodes: List[NodeType], rate: Int, k: Int) extends Actor with ActorHelper {

  class FiniteQueue[A](q: Queue[A]) {
    def enqueueFinite[B >: A](elem: B, maxSize: Int): Queue[B] = {
      var ret = q.enqueue(elem)
      while (ret.size > maxSize) { ret = ret.dequeue._2 }
      ret
    }
  }
  implicit def queue2finitequeue[A](q: Queue[A]) = new FiniteQueue[A](q)

  private val random = new Random()
  private val sleepDuration: Int = ((rate / 60.0) * 1000).toInt
  private val values: mutable.Map[String, Queue[Double]] = mutable.Map()

  override def preStart = {
    context.system.scheduler.schedule((sleepDuration * 2.5) milliseconds, sleepDuration milliseconds)({
      val pushValues: ArrayBuffer[(String, Array[Double])] = ArrayBuffer()
      for (node <- nodes) {
        if (!values.exists(_._1 == node.getNodeId)){
          values.put(node.getNodeId, Queue(random.nextGaussian()))
        } else {
          values(node.getNodeId) = values(node.getNodeId).enqueueFinite(random.nextGaussian(), k)
        }
        pushValues += ((node.getNodeId, values(node.getNodeId).toArray))
      }
      self ! pushValues.iterator
    })
  }

  case class SensorSimulator()

  override def receive = {
    case data: Iterator[(String, Array[Double])] => {
      store[(String, Array[Double])](data)
    }
  }


}