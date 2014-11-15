package ch.unibnf.mcs.sparklisa;

import ch.unibnf.mcs.sparklisa.topology.BasestationType;
import ch.unibnf.mcs.sparklisa.topology.NodeType;
import ch.unibnf.mcs.sparklisa.topology.ObjectFactory;
import ch.unibnf.mcs.sparklisa.topology.Topology;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TopologyHelper {

    private static final java.lang.String HDFS_PREFIX = "hdfs:";

    /**
     * Creates a Map containing (NodeKey, NodeType).
     * Used to get a nodes neighbours (via NodeType) using its key.
     *
     * @param t
     * @return
     */
    public static Map<Integer, NodeType> createNumericalNodeMap(Topology t){
        Map<Integer, NodeType> res = Maps.newHashMap();
        for (NodeType node : t.getNode()){
            int nodeId = Integer.parseInt(node.getNodeId().substring(4));
            res.put(nodeId, node);
        }
        return res;
    }

    /**
     * Reads topology information from a proximity matrix file
     * (i.e. one line per node, containing 1s for connected nodes and 0s for non-related),
     * and translates it into an object representation.
     *
     * @param path
     * @param numberOfBaseStations
     * @return
     * @throws IOException
     */
    public static Topology topologyFromBareFile(String path, Integer numberOfBaseStations) throws IOException {
        Topology topology = new Topology();
        List<String> lines = null;
        if (path.startsWith(HDFS_PREFIX)) {
            lines = readHdfsFile(new Path(path), new Configuration());
        } else {
            lines = Files.readAllLines(Paths.get(path), Charset.defaultCharset());
        }
        Integer numberOfNodes = lines.size();

        if (numberOfNodes % numberOfBaseStations != 0){
            throw new IllegalArgumentException("Number of Nodes must be a multiple of number of BaseStations");
        }

        /*
        * For each line in the file, a node is created.
        * Thereafter, all edges (value==1 in the file) for nodes with lower ids are created.
        * Thus, only links to already existing nodes are created
         */
        for (int i = 0; i < numberOfNodes; i++){
            NodeType node = new NodeType();
            node.setNodeId("node"+(i+1));
            topology.getNode().add(node);
            String[] values = lines.get(i).split(",");

            for(int j = 0; j < values.length; j++){
                if (j < i && Integer.parseInt(values[j]) == 1){
                    topology.getNode().get(j).getNeighbour().add(node.getNodeId());
                    node.getNeighbour().add(topology.getNode().get(j).getNodeId());
                }
            }
        }

        for (int i = 0; i < numberOfBaseStations; i++){
            BasestationType station = new BasestationType();
            station.setStationId("station"+(i+1));
            topology.getBasestation().add(station);
        }

        for (int i = 0; i < numberOfNodes; i++){
            topology.getBasestation().get(i % numberOfBaseStations).getNode().add(topology.getNode().get(i).getNodeId());
        }


        return topology;
    }

    /**
     * Creates an object representation of a simple topology with 16 nodes. Used for testing purposes.
     *
     * @return
     */
    public static Topology createSimpleTopology(){
        ObjectFactory of = new ObjectFactory();
        Topology t = of.createTopology();
        NodeType node1 = of.createNodeType();
        node1.setNodeId("node1");
        NodeType node2 = of.createNodeType();
        node2.setNodeId("node2");
        NodeType node3 = of.createNodeType();
        node3.setNodeId("node3");
        NodeType node4 = of.createNodeType();
        node4.setNodeId("node4");
        NodeType node5 = of.createNodeType();
        node5.setNodeId("node5");
        NodeType node6 = of.createNodeType();
        node6.setNodeId("node6");
        NodeType node7 = of.createNodeType();
        node7.setNodeId("node7");
        NodeType node8 = of.createNodeType();
        node8.setNodeId("node8");
        NodeType node9 = of.createNodeType();
        node9.setNodeId("node9");
        NodeType node10 = of.createNodeType();
        node10.setNodeId("node10");
        NodeType node11 = of.createNodeType();
        node11.setNodeId("node11");
        NodeType node12 = of.createNodeType();
        node12.setNodeId("node12");
        NodeType node13 = of.createNodeType();
        node13.setNodeId("node13");
        NodeType node14 = of.createNodeType();
        node14.setNodeId("node14");
        NodeType node15 = of.createNodeType();
        node15.setNodeId("node15");
        NodeType node16 = of.createNodeType();
        node16.setNodeId("node16");

        node1.getNeighbour().add(node2.getNodeId());
        node2.getNeighbour().add(node1.getNodeId());
        node2.getNeighbour().add(node6.getNodeId());
        node3.getNeighbour().add(node4.getNodeId());
        node4.getNeighbour().add(node3.getNodeId());
        node4.getNeighbour().add(node8.getNodeId());
        node5.getNeighbour().add(node6.getNodeId());
        node6.getNeighbour().add(node2.getNodeId());
        node6.getNeighbour().add(node5.getNodeId());
        node6.getNeighbour().add(node7.getNodeId());
        node6.getNeighbour().add(node10.getNodeId());
        node7.getNeighbour().add(node6.getNodeId());
        node7.getNeighbour().add(node8.getNodeId());
        node8.getNeighbour().add(node4.getNodeId());
        node8.getNeighbour().add(node7.getNodeId());
        node9.getNeighbour().add(node13.getNodeId());
        node10.getNeighbour().add(node6.getNodeId());
        node10.getNeighbour().add(node11.getNodeId());
        node11.getNeighbour().add(node10.getNodeId());
        node11.getNeighbour().add(node12.getNodeId());
        node11.getNeighbour().add(node15.getNodeId());
        node12.getNeighbour().add(node11.getNodeId());
        node12.getNeighbour().add(node16.getNodeId());
        node13.getNeighbour().add(node9.getNodeId());
        node13.getNeighbour().add(node14.getNodeId());
        node14.getNeighbour().add(node13.getNodeId());
        node14.getNeighbour().add(node15.getNodeId());
        node15.getNeighbour().add(node11.getNodeId());
        node15.getNeighbour().add(node14.getNodeId());
        node16.getNeighbour().add(node12.getNodeId());

        BasestationType bs1 = of.createBasestationType();
        bs1.setStationId("station1");
        BasestationType bs2 = of.createBasestationType();
        bs2.setStationId("station2");
        BasestationType bs3 = of.createBasestationType();
        bs3.setStationId("station3");
        BasestationType bs4 = of.createBasestationType();
        bs4.setStationId("station4");

        t.getNode().add(node1);
        t.getNode().add(node2);
        t.getNode().add(node3);
        t.getNode().add(node4);
        t.getNode().add(node5);
        t.getNode().add(node6);
        t.getNode().add(node7);
        t.getNode().add(node8);
        t.getNode().add(node9);
        t.getNode().add(node10);
        t.getNode().add(node11);
        t.getNode().add(node12);
        t.getNode().add(node13);
        t.getNode().add(node14);
        t.getNode().add(node15);
        t.getNode().add(node16);

        bs1.getNode().add(node1.getNodeId());
        bs1.getNode().add(node2.getNodeId());
        bs1.getNode().add(node5.getNodeId());
        bs1.getNode().add(node6.getNodeId());

        bs2.getNode().add(node3.getNodeId());
        bs2.getNode().add(node4.getNodeId());
        bs2.getNode().add(node7.getNodeId());
        bs2.getNode().add(node8.getNodeId());

        bs3.getNode().add(node9.getNodeId());
        bs3.getNode().add(node10.getNodeId());
        bs3.getNode().add(node13.getNodeId());
        bs3.getNode().add(node14.getNodeId());

        bs4.getNode().add(node11.getNodeId());
        bs4.getNode().add(node12.getNodeId());
        bs4.getNode().add(node15.getNodeId());
        bs4.getNode().add(node16.getNodeId());

        t.getBasestation().add(bs1);
        t.getBasestation().add(bs2);
        t.getBasestation().add(bs3);
        t.getBasestation().add(bs4);

        return t;
    }

    /**
     * Utility method to read from HDFS
     */
    private static List<String> readHdfsFile(Path location, Configuration conf) throws IOException {
        FileSystem fileSystem = FileSystem.get(location.toUri(), conf);
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        FileStatus[] items = fileSystem.listStatus(location);
        if (items == null) return new ArrayList<>();
        List<String> results = new ArrayList<>();
        for(FileStatus item: items) {

            // ignoring files like _SUCCESS
            if(item.getPath().getName().startsWith("_")) {
                continue;
            }

            CompressionCodec codec = factory.getCodec(item.getPath());
            BufferedReader br = null;
            InputStream stream = null;

            // check if we have a compression codec we need to use
            if (codec != null) {
                stream = codec.createInputStream(fileSystem.open(item.getPath()));
            }
            else {
                stream = fileSystem.open(item.getPath());
            }

            br = new BufferedReader(new InputStreamReader(stream));
            try {
                String line = br.readLine();
                while (line != null){
                    results.add(line);
                    line = br.readLine();
                }
            } finally {
                br.close();
            }
        }
        return results;
    }
}
