package ch.unibnf.mcs.sparklisa.spark;

import java.io.InputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;
import akka.actor.Props;
import akka.japi.Creator;
import ch.unibnf.mcs.sparklisa.receiver.SensorSimulatorActorReceiver;
import ch.unibnf.mcs.sparklisa.sensor_topology.BasestationType;
import ch.unibnf.mcs.sparklisa.sensor_topology.NodeType;
import ch.unibnf.mcs.sparklisa.sensor_topology.Topology;
import ch.unibnf.mcs.sparklisa.xml.XmlParser;

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

	public static void main(String[] args) throws JAXBException {
		Topology topology = readXml();

		SparkConf conf = createConf();
		JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(1000L));

		for (BasestationType station : topology.getBasestations().getBasestation()) {
			handleBaseStation(station, jssc);
		}

		jssc.start();
		jssc.awaitTermination();
	}

	private static void handleBaseStation(BasestationType station, JavaStreamingContext jssc) {

		for (JAXBElement<Object> nodeObj : station.getManagedNodes().getNode()) {
			handleNode(nodeObj, jssc);
		}
	}

	@SuppressWarnings("serial")
	private static void handleNode(JAXBElement<Object> nodeObj, JavaStreamingContext jssc) {
		NodeType node = (NodeType) nodeObj.getValue();
		Props p = Props.create(new SimpleCreator());
		JavaDStream<Double> values = jssc.actorStream(p, "Node_" + node.getNodeId() + "_Receiver");

		JavaPairDStream<Double, String> valuesMapped = values.map(new PairFunction<Double, Double, String>() {
			@Override
			public Tuple2<Double, String> call(Double d) throws Exception {
				return new Tuple2<Double, String>(d, "a");
			}
		});

		valuesMapped.print();
	}

	private static Topology readXml() throws JAXBException {
		InputStream is = XmlParser.class.getClassLoader().getResourceAsStream("xml/simple_topology.xml");
		JAXBContext context = JAXBContext.newInstance(Topology.class);
		Unmarshaller unmarshaller = context.createUnmarshaller();
		Topology t = (Topology) unmarshaller.unmarshal(is);
		return t;
	}

	@SuppressWarnings("serial")
	static class SimpleCreator implements Creator<SensorSimulatorActorReceiver> {

		@Override
		public SensorSimulatorActorReceiver create() throws Exception {
			return new SensorSimulatorActorReceiver();
		}

	}

}
