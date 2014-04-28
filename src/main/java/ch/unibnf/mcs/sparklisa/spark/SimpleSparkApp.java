package ch.unibnf.mcs.sparklisa.spark;

import javax.xml.bind.JAXBException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;
import akka.actor.Props;
import akka.japi.Creator;
import ch.unibnf.mcs.sparklisa.receiver.SensorSimulatorActorReceiver;
import ch.unibnf.mcs.sparklisa.sensor_topology.NodeType;

public class SimpleSparkApp {

	public static SparkConf createConf() {
		// spark://saight02:7077
		SparkConf conf = new SparkConf();
		conf.setAppName("Simple Streaming App").setMaster("local")
				.setSparkHome("/home/snoooze/spark/spark-0.9.0-incubating-bin-hadoop2")
				.setJars(new String[] { "target/SparkLisa-0.0.1-SNAPSHOT.jar" });
		// conf.s

		return conf;
	}

	@SuppressWarnings("serial")
	public static void main(String[] args) throws JAXBException {

		SparkConf conf = createConf();
		JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(1000L));

		Props p = Props.create(new SimpleCreator());
		JavaDStream<Tuple2<NodeType, Double>> values = jssc.actorStream(p, "Sensor_Receiver");
		JavaDStream<Tuple2<NodeType, Double>> values2 = jssc.actorStream(p, "Sensor_Receiver2");

		JavaDStream<Tuple2<NodeType, Double>> sum = values
				.reduce(new Function2<Tuple2<NodeType, Double>, Tuple2<NodeType, Double>, Tuple2<NodeType, Double>>() {

					@Override
					public Tuple2<NodeType, Double> call(Tuple2<NodeType, Double> t1, Tuple2<NodeType, Double> t2) {
						NodeType t = new NodeType();
						Double sum = t1._2() + t2._2();
						return new Tuple2<NodeType, Double>(t, sum);
					}

				});

		// JavaPairDStream

		// JavaPairDStream<Double, String> valuesMapped = values.map(new
		// PairFunction<Tuple2<NodeType, Double>, Double, String>() {
		//
		// @Override
		// public Tuple2<Double, String> call(Tuple2<NodeType, Double> value)
		// throws Exception {
		//
		// return new Tuple2<Double, String>(value._2(),
		// value._1().getNodeId());
		// }
		// });

		sum.print();

		jssc.start();
		jssc.awaitTermination();
	}

	@SuppressWarnings("serial")
	static class SimpleCreator implements Creator<SensorSimulatorActorReceiver> {

		@Override
		public SensorSimulatorActorReceiver create() throws Exception {
			return new SensorSimulatorActorReceiver();
		}

	}

}
