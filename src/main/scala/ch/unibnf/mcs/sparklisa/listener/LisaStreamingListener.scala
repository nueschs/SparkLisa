package ch.unibnf.mcs.sparklisa.listener

import java.io.File

import org.apache.spark.streaming.scheduler._
import org.slf4j.{LoggerFactory, Logger}

import scala.compat.Platform

/**
 * Created by Stefan Nüesch on 14.08.14.
 */
class LisaStreamingListener extends StreamingListener {
  val Log: Logger = LoggerFactory.getLogger(getClass)

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) = {
    var s : String = ""
    for(f <- new File("/home/stefan").listFiles()){
      s+=f.getName+"\n"
    }
    s += "batch_completed (" + Platform.currentTime.toString + "): \n" + batchTime(batchCompleted.batchInfo)
    Log.info(s)
  }

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted) = {
    Log.info("batch_started (" + Platform.currentTime.toString + "): " + batchStarted.batchInfo.toString)
  }

  override def onReceiverError(receiverError: StreamingListenerReceiverError) = {
    Log.warn("receiver error: " + receiverError.receiverInfo.toString)
  }

  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted) = {
    Log.info("receiver started: " + receiverStarted.receiverInfo.toString)
  }

  private def batchTime(batchInfo: BatchInfo): String = {
    var batchTimeString: String = "\tsubmission_time: " + batchInfo.submissionTime.toString + "\n"
    batchTimeString += "\tscheduling_delay: " + batchInfo.schedulingDelay + "\n"
    batchTimeString += "\tprocessing_start_time: " + batchInfo.processingStartTime + "\n"
    batchTimeString += "\tprocessing_end_time: " + batchInfo.processingEndTime + "\n"
    batchTimeString += "\tprocessing_delay: " + batchInfo.processingDelay + "\n"
    batchTimeString += "\ttotal_delay: " + batchInfo.totalDelay

    return batchTimeString
  }

  /*
  [StreamingListenerBus] INFO ch.unibnf.mcs.sparklisa.listener.LisaStreamingListener -
  batch_started: BatchInfo(1408608640000 ms,Map(),1408608654375,None,None)

  [StreamingListenerBus] INFO ch.unibnf.mcs.sparklisa.listener.LisaStreamingListener -
  batch_completed: BatchInfo(1408608640000 ms,Map(),1408608654375,Some(1408608654380),Some(1408608658703))
   */

}
