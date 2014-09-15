package ch.unibnf.mcs.sparklisa

import ch.unibnf.mcs.sparklisa.receiver.TopologySimulatorActorReceiver
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

/**
 * Created by Stefan Nüesch on 15.09.14.
 */
class SparkLisaRegistrator extends KryoRegistrator{
  override def registerClasses(kryo: Kryo) = {
    kryo.register(classOf[TopologySimulatorActorReceiver])
  }
}
