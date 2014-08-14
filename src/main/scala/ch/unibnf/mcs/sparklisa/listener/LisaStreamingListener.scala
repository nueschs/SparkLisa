package ch.unibnf.mcs.sparklisa.listener

import org.apache.spark.streaming.scheduler._
import org.slf4j.{LoggerFactory, Logger}

/**
 * Created by Stefan Nüesch on 14.08.14.
 */
class LisaStreamingListener extends StreamingListener{
  val Log: Logger = LoggerFactory.getLogger(getClass)

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) = {
    Log.info(batchCompleted.batchInfo.toString)
  }

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted) = {
    Log.info(batchStarted.batchInfo.toString)
  }

  override def onReceiverError(receiverError: StreamingListenerReceiverError) = {
    Log.warn("receiver error: "+receiverError.receiverInfo.toString)
  }

  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted) = {
    Log.info("receiver started: "+receiverStarted.receiverInfo.toString)
  }

}
