package ch.unibnf.mcs.sparklisa;

import ch.unibnf.mcs.sparklisa.topology.BasestationType;
import ch.unibnf.mcs.sparklisa.topology.NodeType;
import ch.unibnf.mcs.sparklisa.topology.ObjectFactory;
import ch.unibnf.mcs.sparklisa.topology.Topology;
import org.w3c.dom.Document;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;

/**
 * Created by snoooze on 17.06.14.
 */
public class TopologyHelper {

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

        node1.getNeighbour().add(node2);
        node2.getNeighbour().add(node1);
        node2.getNeighbour().add(node6);
        node3.getNeighbour().add(node4);
        node4.getNeighbour().add(node3);
        node4.getNeighbour().add(node8);
        node5.getNeighbour().add(node6);
        node6.getNeighbour().add(node2);
        node6.getNeighbour().add(node5);
        node6.getNeighbour().add(node7);
        node6.getNeighbour().add(node10);
        node7.getNeighbour().add(node6);
        node7.getNeighbour().add(node8);
        node8.getNeighbour().add(node4);
        node8.getNeighbour().add(node7);
        node9.getNeighbour().add(node13);
        node10.getNeighbour().add(node6);
        node10.getNeighbour().add(node11);
        node11.getNeighbour().add(node10);
        node11.getNeighbour().add(node12);
        node11.getNeighbour().add(node15);
        node12.getNeighbour().add(node11);
        node12.getNeighbour().add(node16);
        node13.getNeighbour().add(node9);
        node13.getNeighbour().add(node14);
        node14.getNeighbour().add(node13);
        node14.getNeighbour().add(node15);
        node15.getNeighbour().add(node11);
        node15.getNeighbour().add(node14);
        node16.getNeighbour().add(node12);

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

        bs1.getNode().add(node1);
        bs1.getNode().add(node2);
        bs1.getNode().add(node5);
        bs1.getNode().add(node6);

        bs2.getNode().add(node3);
        bs2.getNode().add(node4);
        bs2.getNode().add(node7);
        bs2.getNode().add(node8);

        bs3.getNode().add(node9);
        bs3.getNode().add(node10);
        bs3.getNode().add(node13);
        bs3.getNode().add(node14);

        bs4.getNode().add(node11);
        bs4.getNode().add(node12);
        bs4.getNode().add(node15);
        bs4.getNode().add(node16);

        t.getBasestation().add(bs1);
        t.getBasestation().add(bs2);
        t.getBasestation().add(bs3);
        t.getBasestation().add(bs4);

        return t;
    }
}
