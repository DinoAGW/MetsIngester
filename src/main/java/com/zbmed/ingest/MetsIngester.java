package com.zbmed.ingest;

import java.io.File;
import java.io.InputStream;
import java.io.StringReader;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.xml.sax.InputSource;

import com.exlibris.core.infra.common.util.IOUtil;
import com.exlibris.core.sdk.utils.FileUtil;
import com.exlibris.digitool.deposit.service.xmlbeans.DepositDataDocument;
import com.exlibris.digitool.deposit.service.xmlbeans.DepositResultDocument;
import com.exlibris.digitool.deposit.service.xmlbeans.DepositDataDocument.DepositData;
import com.exlibris.digitool.deposit.service.xmlbeans.DepositResultDocument.DepositResult;
import com.exlibris.dps.DepositWebServices_Service;
import com.exlibris.dps.ProducerWebServices;
import com.exlibris.dps.ProducerWebServices_Service;
import com.exlibris.dps.SipStatusInfo;
import com.exlibris.dps.SipWebServices_Service;
import com.exlibris.dps.sdk.pds.HeaderHandlerResolver;

public class MetsIngester {
	private static final boolean validateXML = true;
	
	static final String fs = System.getProperty("file.separator");
	public static final String sipPath = System.getProperty("user.home").concat(fs).concat("workspace").concat(fs)
			.concat("metsSIPs").concat(fs);
	
	private static final String userName = "SubApp ZB MED";
	private static final String institution = "ZBM";
	private static String password = "";
	private static final String depositSetId = "1";//TODO was könnte das sein?
	
	private static final String ROSETTA_METS_SCHEMA = "http://www.exlibrisgroup.com/xsd/dps/rosettaMets";
	private static final String METS_SCHEMA = "http://www.loc.gov/METS/";
	private static final String XML_SCHEMA = "http://www.w3.org/2001/XMLSchema-instance";
	private static final String XML_SCHEMA_REPLACEMENT = "http://www.exlibrisgroup.com/XMLSchema-instance";
//	private static final String METS_XSD = "mets.xsd";
	private static final String ROSETTA_METS_XSD = "mets_rosetta.xsd";
	
	public static void main(String[] args) throws Exception {
		System.out.println("Starte ingester...");
		ingester();
		System.out.println("Ingester ende");
	}
	
	public static void ingester() throws Exception {
		File sipPathFile = new File(sipPath);
		File[] insts = sipPathFile.listFiles();
		if (insts == null) {
			System.err.println("Keine Unterordner unter '".concat(sipPath).concat("' gefunden."));
			throw new Exception();
		}
		for (File inst : insts) {
			if (inst.isDirectory()) {
				String instName = inst.getName();
				String rosettaURL = null;
				if (instName.equals("dev")) {
					rosettaURL = "http://rosetta.develop.lza.tib.eu";
				} else if (instName.equals("test")) {
					rosettaURL = "http://rosetta.test.lza.tib.eu";
				} else if (instName.equals("prod")) {
					rosettaURL = "http://rosetta.lza.tib.eu";
				} else {
					continue;
				}
				File[] matIds = inst.listFiles();
				for (File matId : matIds) {
					String materialflowId = matId.getName();
					File[] sips = matId.listFiles();
					for (File sip : sips) {
						String  subDirectoryName = sip.getAbsolutePath();
						ingest(subDirectoryName, rosettaURL, materialflowId);
					}
				}
			}
		}
	}

	public static void ingest(String subDirectoryName, String rosettaURL, String materialflowId) throws Exception {
		final String DEPOSIT_WSDL_URL = rosettaURL.concat("/dpsws/deposit/DepositWebServices?wsdl");
		final String PRODUCER_WSDL_URL = rosettaURL.concat("/dpsws/backoffice/ProducerWebServices?wsdl");
		String SIP_STATUS_WSDL_URL = rosettaURL.concat("/dpsws/repository/SipWebServices?wsdl");
		
		if (validateXML) {			
			final String IEfullFileName = subDirectoryName.concat(fs).concat("content").concat(fs).concat("ie1.xml");
	
			//validate against mets_rosetta.xsd
			File ieXML = new File(IEfullFileName);
			System.out.println(ieXML.exists());
			String xmlMetsContent = FileUtil.getFileContent(IEfullFileName);
			
			if (xmlMetsContent == null) {
				System.err.println("IE XML ist leer: '".concat(IEfullFileName).concat("'"));
				throw new Exception();
			}
	
			//Need to replace manually the namespace with Rosetta Mets schema in order to pass validation against mets_rosetta.xsd
			String xmlRosettaMetsContent = xmlMetsContent.replaceAll(XML_SCHEMA, XML_SCHEMA_REPLACEMENT);
			xmlRosettaMetsContent = xmlMetsContent.replaceAll(METS_SCHEMA, ROSETTA_METS_SCHEMA);
	
			validateXML(ieXML.getAbsolutePath(), xmlRosettaMetsContent, ROSETTA_METS_XSD);
		}

		// 3. Place the SIP directory in a folder that can be accessed by the Rosetta application (using FTP is a valid approach)
		URL ProdWsdlUrl = new URL(PRODUCER_WSDL_URL);
		QName ExlPWS = new QName("http://dps.exlibris.com/", "ProducerWebServices");
		ProducerWebServices producerWebServices = new ProducerWebServices_Service(ProdWsdlUrl, ExlPWS).getProducerWebServicesPort();
		String producerAgentId = producerWebServices.getInternalUserIdByExternalId(userName);
		String xmlReply = producerWebServices.getProducersOfProducerAgent(producerAgentId);
		DepositDataDocument depositDataDocument = DepositDataDocument.Factory.parse(xmlReply);
		DepositData depositData = depositDataDocument.getDepositData();

		String producerId = depositData.getDepDataArray(0).getId();
		System.out.println("Producer ID: " + producerId);
		//submit

		// 4. Set Authentication Header on service
		DepositWebServices_Service depWS = new DepositWebServices_Service(new URL(DEPOSIT_WSDL_URL),
				new QName("http://dps.exlibris.com/", "DepositWebServices"));
		depWS.setHandlerResolver(new HeaderHandlerResolver(userName, password, institution));

		// 5. Submit Deposit
		String retval = depWS.getDepositWebServicesPort().submitDepositActivity(null, materialflowId, subDirectoryName,
				producerId, depositSetId);
		System.out.println("Submit Deposit Result: " + retval);

		DepositResultDocument depositResultDocument = DepositResultDocument.Factory.parse(retval);
		DepositResult depositResult = depositResultDocument.getDepositResult();

		// 6.check status of sip when deposit was successful
		for (int i = 0; i < 1; i++) {
			Thread.sleep(3000);//wait until deposit is in
			if (depositResult.getIsError()) {
				System.out.println("Submit Deposit Failed");
			} else {
				SipStatusInfo status = new SipWebServices_Service(new URL(SIP_STATUS_WSDL_URL),
						new QName("http://dps.exlibris.com/", "SipWebServices")).getSipWebServicesPort()
								.getSIPStatusInfo(String.valueOf(depositResult.getSipId()));
				System.out.println("Submitted Deposit Status: " + status.getStatus());
				System.out.println("Submitted Deposit Stage: " + status.getStage());
				System.out.println("Submitted Deposit is in Module: " + status.getModule());
			}
		}
	}

	private static void validateXML(String fileFullName, String xml, String xsdName) throws Exception {
		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			factory.setNamespaceAware(true);
			factory.setSchema(getSchema(xsdName));
			DocumentBuilder builder = factory.newDocumentBuilder();
			builder.parse(new InputSource(new StringReader(xml)));
		} catch (Exception e) {
			System.out.println("XML '" + fileFullName + "' doesn't pass validation by :" + xsdName
					+ " with the following validation error: " + e.getMessage());
		}
	}

	private static Schema getSchema(String xsdName) throws Exception {
		Map<String, Schema> schemas = new HashMap<String, Schema>();
		if (schemas.get(xsdName) == null) {
			InputStream inputStream = null;
			try {
				File xsd = new File("src/xsd/mets_rosetta.xsd");
				Source xsdFile = new StreamSource(xsd);
				SchemaFactory schemaFactory = SchemaFactory.newInstance("http://www.w3.org/XML/XMLSchema/v1.1");
				schemas.put(xsdName, schemaFactory.newSchema(xsdFile));
			} catch (Exception e) {
				System.out.println("Failed to create Schema with following error: " + e.getMessage());
			} finally {
				IOUtil.closeQuietly(inputStream);
			}
		}
		return schemas.get(xsdName);
	}
}
