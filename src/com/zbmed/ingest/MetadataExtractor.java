package com.zbmed.ingest;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.DocumentBuilder;

import org.dom4j.QName;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.exlibris.core.sdk.formatting.DublinCore;

import org.w3c.dom.Node;
import org.w3c.dom.Element;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;

public class MetadataExtractor {
	static final String fs = System.getProperty("file.separator");
	static final String folder_on_working_machine = System.getProperty("user.home").concat(fs).concat("workspace");
	static final String filesRootFolder = folder_on_working_machine.concat(fs).concat("SRU").concat(fs);

	public static void extractMetadata(DublinCore dc, String HT)
			throws ParserConfigurationException, SAXException, IOException {
		final String MetadataFilePath = filesRootFolder.concat(HT).concat(".xml");
		final File MetadataFile = new File(MetadataFilePath);
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(MetadataFile);
		doc.getDocumentElement().normalize();

		NodeList nodes = doc.getElementsByTagName("identifier");
		for (int i = 0; i < nodes.getLength(); ++i) {
			if (nodes.item(i).getTextContent().startsWith("http")) {
				//dc.addElement("dc:identifier@dcterms:URI", nodes.item(i).getTextContent());
			} else {
				dc.addElement("dc:identifier", nodes.item(i).getTextContent());
			}
		}
		nodes = doc.getElementsByTagName("title");
		for (int i = 0; i < nodes.getLength(); ++i) {
			dc.addElement("dc:title", nodes.item(i).getTextContent());
		}
		nodes = doc.getElementsByTagName("creator");
		for (int i = 0; i < nodes.getLength(); ++i) {
			dc.addElement("dc:creator", nodes.item(i).getTextContent());
		}
		nodes = doc.getElementsByTagName("publisher");
		for (int i = 0; i < nodes.getLength(); ++i) {
			dc.addElement("dc:publisher", nodes.item(i).getTextContent());
		}
		nodes = doc.getElementsByTagName("date");
		for (int i = 0; i < nodes.getLength(); ++i) {
			dc.addElement("dc:date", nodes.item(i).getTextContent());
		}
		dc.addElement("dc:type", "conference object");
		dc.addElement("dc:format", "1 Online-Ressource");
	}

	public static void main(String[] args) throws ParserConfigurationException, SAXException, IOException {

		File streamDir = new File(filesRootFolder);
		File[] files = streamDir.listFiles();
		int[] min = { 999, 999, 999 };
		int[] max = { 0, 0, 0 };

		Writer bw = new BufferedWriter(new FileWriter(folder_on_working_machine.concat(fs).concat("titles.csv")));
		PrintWriter pw = new PrintWriter(bw);
		pw.println("\"HT Nummer\",\"title(0); title(1)\",\"title(0)\",\"title(1)\",\"title(2)\"");
		for (int i = 0; i < files.length; i++) {
			final String MetadataFilePath = filesRootFolder.concat(files[i].getName());
			final File MetadataFile = new File(MetadataFilePath);

			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(MetadataFile);

			doc.getDocumentElement().normalize();

			//		System.out.println("Root element :" + doc.getDocumentElement().getNodeName());
			//		System.out.println("Root element :" + doc.getDocumentElement().getBaseURI());
			//		System.out.println("Root element :" + doc.getDocumentElement().getLocalName());
			//		System.out.println("Root element :" + doc.getDocumentElement().getNamespaceURI());
			//		System.out.println("Root element :" + doc.getDocumentElement().getNodeType());
			//		System.out.println("Root element :" + doc.getDocumentElement().getNodeValue());
			//		System.out.println("Root element :" + doc.getDocumentElement().getPrefix());
			//		System.out.println("Root element :" + doc.getDocumentElement().getTagName());
			//		System.out.println("Root element :" + doc.getDocumentElement().getTextContent());
			//		System.out.println("Root element :" + doc.getDocumentElement().getAttributes());

			NodeList nodes = doc.getElementsByTagName("title");

			String[] item = new String[3];
			item[0] = nodes.item(0).getTextContent().replace("\"", "\"\"");
			item[1] = nodes.item(1).getTextContent().replace("\"", "\"\"");
			if (nodes.getLength() > 2) {
				item[2] = nodes.item(2).getTextContent().replace("\"", "\"\"");
			} else {
				item[2] = "";
			}
			pw.println("\"" + files[i].getName() + "\",\"" + item[0] + "; " + item[1] + "\",\"" + item[0] + "\",\""
					+ item[1] + "\",\"" + item[2] + "\"");
			//			if (nodes.getLength() != 2)
			System.out.println("Datei: " + files[i].getName());

			if (nodes.item(0).getTextContent().length() < 30)
				System.out.println(
						"kurze 0: (" + nodes.item(0).getTextContent().length() + ") " + nodes.item(0).getTextContent());
			if (nodes.item(1).getTextContent().length() >= 30)
				System.out.println(
						"lange 1: (" + nodes.item(1).getTextContent().length() + ") " + nodes.item(1).getTextContent());
			for (int i2 = 0; i2 < nodes.getLength(); ++i2) {
				if (nodes.item(i2).getTextContent().length() < min[i2])
					min[i2] = nodes.item(i2).getTextContent().length();
				if (nodes.item(i2).getTextContent().length() > max[i2])
					max[i2] = nodes.item(i2).getTextContent().length();
				if (nodes.getLength() != 2)
					System.out.println("Node: " + nodes.item(i2).getTextContent());
			}
		}
		pw.close();
		for (int i = 0; i < 3; i++) {
			System.out.println("Node #" + i + " zwischen " + min[i] + " und " + max[i] + ".");
		}
		System.out.println("MetadataExtractor Ende");
	}

}
