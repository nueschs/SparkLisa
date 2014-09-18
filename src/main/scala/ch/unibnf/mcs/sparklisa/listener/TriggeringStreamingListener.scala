package ch.unibnf.mcs.sparklisa.listener

import java.net.InetSocketAddress

import akka.actor.{Props, ActorRef, Actor, ActorSystem}
import akka.io.Tcp
import akka.io.IO
import akka.util.ByteString
import org.apache.spark.streaming.scheduler._
import org.slf4j.{Logger, LoggerFactory}

import scala.compat.Platform

class TriggeringStreamingListener extends StreamingListener{
  val Log: Logger = LoggerFactory.getLogger(getClass)
  val Port = 23456
  var ReceiverActor: ActorRef = null
//  var ActorActivators: mutable.MutableList[ListenerActor] = mutable.MutableList()

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) = {
    ReceiverActor ! ByteString.fromString("trigger")
    Log.info("batch_completed ("+Platform.currentTime.toString+"): \n"+batchTime(batchCompleted.batchInfo))
  }

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted) = {
    Log.info("batch_started ("+Platform.currentTime.toString+"): "+batchStarted.batchInfo.toString)
  }

  override def onReceiverError(receiverError: StreamingListenerReceiverError) = {
    Log.warn("receiver error: "+receiverError.receiverInfo.toString)
  }

  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted) = {
    val location = receiverStarted.receiverInfo.location
    val system = ActorSystem("ReceiverSystem")
    ReceiverActor =  system.actorOf(TriggeringActor.props(new InetSocketAddress(location, Port)))
    ReceiverActor ! ByteString.fromString("trigger")
    Log.info("receiver started: "+receiverStarted.receiverInfo.toString)
  }

  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped) = {
    ReceiverActor ! "close"
    Log.info("receiver stopped: "+receiverStopped.receiverInfo.toString)
  }

  private def batchTime(batchInfo : BatchInfo): String = {
    var batchTimeString : String = "\tsubmission_time: "+batchInfo.submissionTime.toString+"\n"
    batchTimeString += "\tscheduling_delay: "+batchInfo.schedulingDelay+"\n"
    batchTimeString += "\tprocessing_start_time: "+batchInfo.processingStartTime+"\n"
    batchTimeString += "\tprocessing_end_time: "+batchInfo.processingEndTime+"\n"
    batchTimeString += "\tprocessing_delay: "+batchInfo.processingDelay+"\n"
    batchTimeString += "\ttotal_delay: "+batchInfo.totalDelay
    batchTimeString += "\tblock_info: "+(batchInfo.receivedBlockInfo mkString(";"))
    return batchTimeString
  }

  object TriggeringActor {
    def props(remote: InetSocketAddress) = {
      Props(new TriggeringActor(remote))
    }
  }

  class TriggeringActor(remote: InetSocketAddress) extends Actor {
    import Tcp._
    import context.system

    IO(Tcp) ! Connect(remote)

    override def receive = {
      case c @ Connected(remote, local) => {
        val connection = sender
        connection ! Register(self)
        context become {
          case data: ByteString => {
//            Log.info(">>> Sending Trigger")
            connection ! Write(data)
          }
          case "close" => connection ! Close
        }
      }
    }
  }

}
