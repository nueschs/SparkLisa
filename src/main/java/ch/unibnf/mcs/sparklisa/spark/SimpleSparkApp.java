package ch.unibnf.mcs.sparklisa.spark;

import java.io.InputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.*;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;
import akka.actor.Props;
import akka.japi.Creator;
import ch.unibnf.mcs.sparklisa.receiver.SensorSimulatorActorReceiver;
import ch.unibnf.mcs.sparklisa.topology.NodeType;
import ch.unibnf.mcs.sparklisa.topology.Topology;

public class SimpleSparkApp {

    private static final String COUNT_KEY = "COUNT_KEY";
    private static final String SUM_KEY = "SUM_KEY";
    private static final String MEAN_KEY = "MEAN_KEY";
    private static final String ERROR_KEY = "ERROR_KEY";

    public static SparkConf createConf() {
        // spark://saight02:7077
        SparkConf conf = new SparkConf();
        conf.setAppName("Simple Streaming App").setMaster("local")
                .setSparkHome("/home/snoooze/spark/spark-0.9.0-incubating-bin-hadoop2")
                .setJars(new String[]{"target/SparkLisa-0.0.1-SNAPSHOT.jar"});
        // conf.s

        return conf;
    }

    @SuppressWarnings("serial")
    public static void main(String[] args) throws JAXBException {

        SparkConf conf = createConf();
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(5000L));
//        jssc.checkpoint("/home/snoooze/scala_ws/SparkLisa/target/checkpoint");

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

        JavaDStream<Long> runningCount = allValues.count();
        JavaDStream<Double> runningSum = createRunningSum(allValues);


        JavaDStream<Double> runningMean = runningCount.transformWith(runningSum, new Function3<JavaRDD<Long>, JavaRDD<Double>, Time, JavaRDD<Double>>() {
            @Override
            public JavaRDD<Double> call(JavaRDD<Long> countRDD, JavaRDD<Double> sumRDD, Time time) throws Exception {
                JavaPairRDD<Long, Double> cartRDD = countRDD.cartesian(sumRDD);
                return cartRDD.map(new Function<Tuple2<Long, Double>, Double>() {
                    @Override
                    public Double call(Tuple2<Long, Double> sumAndCount) throws Exception {
                        Double sum = sumAndCount._2();
                        Double count = sumAndCount._1().doubleValue();
                        return sum / count;
                    }
                });
            }
        });


        final JavaDStream<Double> meanDiff = allValues.transformWith(runningMean, new Function3<JavaRDD<Tuple2<NodeType, Double>>, JavaRDD<Double>, Time, JavaRDD<Double>>() {
            @Override
            public JavaRDD<Double> call(JavaRDD<Tuple2<NodeType, Double>> val, final JavaRDD<Double> mean, Time time) throws Exception {
                final Double mn = mean.reduce(new Function2<Double, Double, Double>() {
                    @Override
                    public Double call(Double d1, Double d2) throws Exception {
                        return d1+d2;
                    }
                });

                return val.map(new Function<Tuple2<NodeType, Double>, Double>() {
                    @Override
                    public Double call(Tuple2<NodeType, Double> val1) throws Exception {
                        double diff = val1._2() - mn;
                        return Math.pow(diff, 2.0);
                    }
                });
            }
        });

        JavaDStream<Double> stdDev = meanDiff.transformWith(runningCount, new Function3<JavaRDD<Double>, JavaRDD<Long>, Time, JavaRDD<Double>>() {
            @Override
            public JavaRDD<Double> call(JavaRDD<Double> diff, JavaRDD<Long> count, Time time) throws Exception {
                final Double diffSum = diff.reduce(new Function2<Double, Double, Double>() {
                    @Override
                    public Double call(Double d1, Double d2) throws Exception {
                        return d1 + d2;
                    }
                });

                return count.map(new Function<Long, Double>() {
                    @Override
                    public Double call(Long cnt) throws Exception {
                        return Math.sqrt(diffSum/cnt.doubleValue());
                    }
                });
            }
        });

        JavaPairDStream<NodeType, Double> node1LisaValues = createLisaValues(node1Values, runningMean, stdDev);
        JavaPairDStream<NodeType, Double> node2LisaValues = createLisaValues(node2Values, runningMean, stdDev);
        JavaPairDStream<NodeType, Double> node3LisaValues = createLisaValues(node3Values, runningMean, stdDev);
        JavaPairDStream<NodeType, Double> node4LisaValues = createLisaValues(node4Values, runningMean, stdDev);

        JavaPairDStream<NodeType, Double> node1Lisa = createNodeLisa(node1LisaValues, node2LisaValues, node4LisaValues);




        jssc.start();
        jssc.awaitTermination();
    }

    private static JavaPairDStream<NodeType,Double> createNodeLisa(JavaPairDStream<NodeType, Double> node1LisaValues, JavaPairDStream<NodeType, Double>... neighbourVals ) {
        int k = neighbourVals.length;



        return null;
    }

    private static JavaPairDStream<NodeType,Double> createLisaValues(JavaDStream<Tuple2<NodeType, Double>> nodeValues, JavaDStream<Double> runningMean, JavaDStream<Double> stdDev) {
        JavaPairDStream<NodeType, Double> meanDiff  = nodeValues.transformWith(runningMean, new Function3<JavaRDD<Tuple2<NodeType, Double>>, JavaRDD<Double>, Time, JavaPairRDD<NodeType, Double>>() {
            @Override
            public JavaPairRDD<NodeType, Double> call(final JavaRDD<Tuple2<NodeType, Double>> nodeVal, JavaRDD<Double> mean, Time time) throws Exception {
                return nodeVal.cartesian(mean).map(new PairFunction<Tuple2<Tuple2<NodeType, Double>, Double>, NodeType, Double>() {
                    @Override
                    public Tuple2<NodeType, Double> call(Tuple2<Tuple2<NodeType, Double>, Double> cart) throws Exception {
                        Double meanDiff = cart._1()._2() - cart._2();
                        return new Tuple2<>(cart._1()._1(), meanDiff);
                    }
                });
            }
        });

        return meanDiff.transformWith(stdDev, new Function3<JavaPairRDD<NodeType, Double>, JavaRDD<Double>, Time, JavaPairRDD<NodeType, Double>>() {
            @Override
            public JavaPairRDD<NodeType, Double> call(JavaPairRDD<NodeType, Double> nodeDiff, JavaRDD<Double> dev, Time time) throws Exception {
                return nodeDiff.cartesian(dev).map(new PairFunction<Tuple2<Tuple2<NodeType, Double>, Double>, NodeType, Double>() {
                    @Override
                    public Tuple2<NodeType, Double> call(Tuple2<Tuple2<NodeType, Double>, Double> val) throws Exception {
                        NodeType node = val._1()._1();
                        Double resVal = val._1()._2()/val._2();
                        return new Tuple2<>(node, resVal);
                    }
                });
            }
        });
    }


    private static JavaDStream<Double> createRunningSum(JavaDStream<Tuple2<NodeType, Double>> allValues){
        JavaPairDStream<String, Double> mapped = allValues.map(new PairFunction<Tuple2<NodeType, Double>, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Tuple2<NodeType, Double> value) {
                return new Tuple2<>(SUM_KEY, value._2());
            }
        });

        JavaPairDStream<String, Double> sumMapped = mapped.reduceByKey(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double d1, Double d2) {
                return d1 + d2;
            }
        });

        return sumMapped.map(new Function<Tuple2<String, Double>, Double>() {
            @Override
            public Double call(Tuple2<String, Double> sumMap) throws Exception {
                return sumMap._2();
            }
        });
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
