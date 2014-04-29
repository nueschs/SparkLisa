package ch.unibnf.mcs.sparklisa.spark;

import java.io.InputStream;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;
import akka.actor.Props;
import akka.japi.Creator;
import ch.unibnf.mcs.sparklisa.receiver.SensorSimulatorActorReceiver;
import ch.unibnf.mcs.sparklisa.spark.SimpleSparkApp.SimpleCreator;
import ch.unibnf.mcs.sparklisa.topology.NodeType;
import ch.unibnf.mcs.sparklisa.topology.Topology;

import com.google.common.base.Optional;

public class SimpleSparkApp {

	private static final String COUNT_KEY = "COUNT_KEY";
	private static final String SUM_KEY = "SUM_KEY";

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
		jssc.checkpoint("/home/snoooze/scala_ws/SpatkLisa/target/checkpoint");

		Topology topology = readXml();

		NodeType node1 = new NodeType();
		node1.setNodeId("node1");
		NodeType node2 = new NodeType();
		node2.setNodeId("node2");
		NodeType node3 = new NodeType();
		node3.setNodeId("node3");
		NodeType node4 = new NodeType();
		node4.setNodeId("node4");

		node1.getNeighbour().add(node2);
		node2.getNeighbour().add(node1);
		node2.getNeighbour().add(node3);
		node3.getNeighbour().add(node2);
		node3.getNeighbour().add(node4);
		node4.getNeighbour().add(node3);

		Props node1Props = Props.create(new SimpleCreator(node1));
		Props node2Props = Props.create(new SimpleCreator(node2));
		Props node3Props = Props.create(new SimpleCreator(node3));
		Props node4Props = Props.create(new SimpleCreator(node4));

		JavaDStream<Tuple2<NodeType, Double>> node1Values = jssc.actorStream(node1Props, "Node1_Receiver");
		JavaDStream<Tuple2<NodeType, Double>> node2Values = jssc.actorStream(node2Props, "Node2_Receiver");
		JavaDStream<Tuple2<NodeType, Double>> node3Values = jssc.actorStream(node3Props, "Node3_Receiver");
		JavaDStream<Tuple2<NodeType, Double>> node4Values = jssc.actorStream(node4Props, "Node4_Receiver");

		JavaDStream<Tuple2<NodeType, Double>> allValues = node1Values.union(node2Values).union(node3Values).union(node4Values);

		JavaPairDStream<String, Long> runningCount = createRunningCount(allValues);
		JavaPairDStream<String, Double> runningSum = createRunningSum(allValues);

		runningCount.print();
		runningSum.print();

		jssc.start();
		jssc.awaitTermination();
	}

	private static JavaPairDStream<String, Long> createRunningCount(JavaDStream<Tuple2<NodeType, Double>> allValues) {
		JavaDStream<Long> count = allValues.count();

		JavaPairDStream<String, Long> countMapped = count.map(new PairFunction<Long, String, Long>() {
			public Tuple2<String, Long> call(Long l) {
				return new Tuple2<String, Long>(COUNT_KEY, l);
			}
		});

		Function2<List<Long>, Optional<Long>, Optional<Long>> reduceFunc = new Function2<List<Long>, Optional<Long>, Optional<Long>>() {
			@Override
			public Optional<Long> call(List<Long> values, Optional<Long> state) {
				Long newSum = state.or(0L);
				for (Long l : values) {
					newSum += l;
				}
				return Optional.of(newSum);
			}
		};
		return countMapped.updateStateByKey(reduceFunc);
	}

	private static JavaPairDStream<String, Double> createRunningSum(JavaDStream<Tuple2<NodeType, Double>> allValues) {
		JavaPairDStream<String, Double> mapped = allValues.map(new PairFunction<Tuple2<NodeType, Double>, String, Double>() {
			public Tuple2<String, Double> call(Tuple2<NodeType, Double> value) {
				return new Tuple2<String, Double>(SUM_KEY, value._2);
			}
		});

		JavaPairDStream<String, Double> sum = mapped.reduceByKey(new Function2<Double, Double, Double>() {
			public Double call(Double d1, Double d2) {
				return d1 + d2;
			}
		});

		Function2<List<Double>, Optional<Double>, Optional<Double>> reduceFunc = new Function2<List<Double>, Optional<Double>, Optional<Double>>() {
			@Override
			public Optional<Double> call(List<Double> values, Optional<Double> state) {
				Double newSum = state.or(0.0);
				for (Double d : values) {
					newSum += d;
				}
				return Optional.of(newSum);
			}
		};

		return sum.updateStateByKey(reduceFunc);
	}

	private static Topology readXml() throws JAXBException {
		InputStream is = SimpleSparkApp.class.getClassLoader().getResourceAsStream("xml/simple_topology.xml");
		JAXBContext context = JAXBContext.newInstance(Topology.class);
		Unmarshaller unmarshaller = context.createUnmarshaller();
		Topology t = (Topology) unmarshaller.unmarshal(is);
		return t;
	}

	@SuppressWarnings("serial")
	static class SimpleCreator implements Creator<SensorSimulatorActorReceiver> {

		private final NodeType node;

		public SimpleCreator(NodeType node) {
			this.node = node;
		}

		@Override
		public SensorSimulatorActorReceiver create() throws Exception {
			return new SensorSimulatorActorReceiver(this.node);
		}

	}

}
