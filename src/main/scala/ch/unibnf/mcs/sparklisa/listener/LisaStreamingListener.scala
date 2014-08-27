package ch.unibnf.mcs.sparklisa.listener

import org.apache.spark.streaming.scheduler._
import org.slf4j.{LoggerFactory, Logger}

import scala.compat.Platform

/**
 * Created by Stefan Nüesch on 14.08.14.
 */
class LisaStreamingListener extends StreamingListener{
  val Log: Logger = LoggerFactory.getLogger(getClass)

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) = {
    Log.info("batch_completed ("+Platform.currentTime.toString+"): \n"+batchTime(batchCompleted.batchInfo))
  }

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted) = {
    Log.info("batch_started ("+Platform.currentTime.toString+"): "+batchStarted.batchInfo.toString)
  }

  override def onReceiverError(receiverError: StreamingListenerReceiverError) = {
    Log.warn("receiver error: "+receiverError.receiverInfo.toString)
  }

  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted) = {
    Log.info("receiver started: "+receiverStarted.receiverInfo.toString)
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
}
