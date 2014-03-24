package ch.unibnf.mcs.sparklisa.xml;

import java.io.IOException;
import java.io.InputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import ch.unibnf.mcs.sparklisa.sensor_topology.NodeType;
import ch.unibnf.mcs.sparklisa.sensor_topology.Topology;

/**
 * 
 * @author Stefan Nüesch
 * 
 */
public class XmlParser {

	public static void main(String[] args) throws SAXException, IOException, ParserConfigurationException, JAXBException {
		InputStream is = XmlParser.class.getClassLoader().getResourceAsStream("xml/simple_topology.xml");

		JAXBContext context = JAXBContext.newInstance(Topology.class);

		Unmarshaller unmarshaller = context.createUnmarshaller();
		Topology t = (Topology) unmarshaller.unmarshal(is);

		NodeType node1 = t.getNodes().getNode().get(0);

		System.out.println(node1.getNeighbours().getNeighbour().get(0).getValue());
	}

}
