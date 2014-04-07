package ch.unibnf.mcs.sparklisa.receiver

import org.apache.spark.streaming.dstream.NetworkReceiver
import org.apache.spark.storage.StorageLevel
import scala.util.Random

class SensorSimulatorNetworkReceiver extends NetworkReceiver[Double] {

  protected lazy val blockGenerator = new BlockGenerator(StorageLevel.MEMORY_ONLY_2)
  private final val random = new Random()

  protected def onStart() = {
    blockGenerator.start()
    while (true) {
      Thread.sleep(50L);
      blockGenerator += random.nextGaussian()
    }
  }

  protected def onStop() = {
    blockGenerator.stop()
  }

}