package ingest;

import gov.loc.mets.DivType;
import gov.loc.mets.FileType;
import gov.loc.mets.MetsDocument;
import gov.loc.mets.MetsDocument.Mets;
import gov.loc.mets.MetsType.FileSec.FileGrp;
import gov.loc.mets.StructMapType;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.xmlbeans.XmlOptions;
import org.xml.sax.InputSource;

import com.exlibris.core.infra.common.util.IOUtil;
import com.exlibris.core.sdk.consts.Enum;
import com.exlibris.core.sdk.formatting.DublinCore;
import com.exlibris.core.sdk.utils.FileUtil;
import com.exlibris.digitool.common.dnx.DnxDocument;
import com.exlibris.digitool.common.dnx.DnxDocumentFactory;
import com.exlibris.digitool.common.dnx.DnxDocumentHelper;
import com.exlibris.digitool.deposit.service.xmlbeans.DepositDataDocument;
import com.exlibris.digitool.deposit.service.xmlbeans.DepositDataDocument.DepositData;
import com.exlibris.digitool.deposit.service.xmlbeans.DepositResultDocument;
import com.exlibris.digitool.deposit.service.xmlbeans.DepositResultDocument.DepositResult;
import com.exlibris.dps.DepositWebServices_Service;
import com.exlibris.dps.ProducerWebServices;
import com.exlibris.dps.ProducerWebServices_Service;
import com.exlibris.dps.SipStatusInfo;
import com.exlibris.dps.SipWebServices_Service;
import com.exlibris.dps.sdk.deposit.IEParser;
import com.exlibris.dps.sdk.deposit.IEParserFactory;
import com.exlibris.dps.sdk.pds.HeaderHandlerResolver;

public class Test {
	static final String userName = "SubApp ZB MED";
	static final String institution = "ZBM";
	static String password = "";
	static final String materialflowId = "76661659";
	static final String depositSetId = "1";

	//should be placed under where submission format of MF is configured
	static final String folder_on_working_machine = System.getProperty("user.home").concat("/workspace");
	static final String subDirectoryName = folder_on_working_machine.concat("/DepositExample3");
	static final String filesRootFolder = subDirectoryName + "/content/streams/";
	static final String IEfullFileName = subDirectoryName + "/content/ie1.xml";

	static final String DEPOSIT_WSDL_URL = "http://rosetta.develop.lza.tib.eu/dpsws/deposit/DepositWebServices?wsdl";
	static final String PRODUCER_WSDL_URL = "http://rosetta.develop.lza.tib.eu/dpsws/backoffice/ProducerWebServices?wsdl";
	static final String SIP_STATUS_WSDL_URL = "http://rosetta.develop.lza.tib.eu/dpsws/repository/SipWebServices?wsdl";

	public static final String ROSETTA_METS_SCHEMA = "http://www.exlibrisgroup.com/xsd/dps/rosettaMets";
	public static final String METS_SCHEMA = "http://www.loc.gov/METS/";
	public static final String XML_SCHEMA = "http://www.w3.org/2001/XMLSchema-instance";
	public static final String XML_SCHEMA_REPLACEMENT = "http://www.exlibrisgroup.com/XMLSchema-instance";
	public static final String METS_XSD = "mets.xsd";
	public static final String ROSETTA_METS_XSD = "mets_rosetta.xsd";

	/**
	 * Full Flow Example with all stages to create and make a Deposit.
	 * @throws IOException 
	 *
	 */

	public static void main(String[] args) throws IOException {
		System.out.print(new String("Passwort des Users ").concat(userName).concat(":"));
		//Passwort abfragen
		Scanner sc = new Scanner(System.in);
		password = sc.next();
		sc.close();
		
		String HT = "HT020488506";
		String ID = "dgnc2020";

		org.apache.log4j.helpers.LogLog.setQuietMode(true);

		try {
			// 1. Create a SIP directory

			// 2. Create the IE using IE parser

			//list of files we are depositing
			File streamDir = new File(filesRootFolder);
			File[] files = streamDir.listFiles();

			//create parser
			IEParser ie = IEParserFactory.create();

			// add ie dc
			DublinCore dc = ie.getDublinCoreParser();
			MetadataExtractor.extractMetadata(dc, HT);
			ie.setIEDublinCore(dc);
			List<FileGrp> fGrpList = new ArrayList<FileGrp>();

			// add fileGrp
			FileGrp fGrp = ie.addNewFileGrp(Enum.UsageType.VIEW, Enum.PreservationType.PRESERVATION_MASTER);

			// add dnx - A new DNX is constructed and added on the file group level
			DnxDocument dnxDocument = ie.getFileGrpDnx(fGrp.getID());
			DnxDocumentHelper documentHelper = new DnxDocumentHelper(dnxDocument);
			documentHelper.getGeneralRepCharacteristics().setRevisionNumber("1");
			documentHelper.getGeneralRepCharacteristics().setLabel(ID);

			//adding an event to the rep DNX
//			List<DnxDocumentHelper.Event> eventList = new ArrayList<DnxDocumentHelper.Event>();
//			DnxDocumentHelper.Event event = documentHelper.new Event();
//			event.setEventIdentifierValue("Test");
//			eventList.add(event);
//			documentHelper.setEvents(eventList);

			ie.setFileGrpDnx(documentHelper.getDocument(), fGrp.getID());

			fGrpList.add(fGrp);
			System.out.println("Directory: " + streamDir.getAbsolutePath() + " with " + files.length + " Files");

			
			for (int i = 0; i < files.length; i++) {

				//add file and dnx metadata on file
				String mimeType = "application/pdf";
				FileType fileType = ie.addNewFile(fGrp, mimeType, files[i].getName(), "lskdjfioij " + i);

				// add dnx - A new DNX is constructed and added on the file level
				DnxDocument dnx = ie.getFileDnx(fileType.getID());
				DnxDocumentHelper fileDocumentHelper = new DnxDocumentHelper(dnx);
//				fileDocumentHelper.getGeneralFileCharacteristics().setNote("note to test");
				fileDocumentHelper.getGeneralFileCharacteristics().setLabel(files[i].getName());
				fileDocumentHelper.getGeneralFileCharacteristics().setFileOriginalPath(files[i].getAbsolutePath().substring(folder_on_working_machine.length()));
				ie.setFileDnx(fileDocumentHelper.getDocument(), fileType.getID());
			}

			ie.generateChecksum(filesRootFolder, Enum.FixityType.MD5.toString());
			ie.updateSize(filesRootFolder);
			
			DnxDocument ieDnx = DnxDocumentFactory.getInstance().createDnxDocument();
			DnxDocumentHelper ieDnxHelper = new DnxDocumentHelper(ieDnx);
			DnxDocumentHelper.CMS cms = ieDnxHelper. new CMS();
			cms.setSystem("HBZ01");
			cms.setRecordId(HT);
			ieDnxHelper.setCMS(cms);
			ie.setIeDnx(ieDnxHelper.getDocument());
			
			//example for adding a logical Struct Map.
			MetsDocument metsDoc = MetsDocument.Factory.parse(ie.toXML());

			//insert IE created in content directory
			File ieXML = new File(IEfullFileName);
			XmlOptions opt = new XmlOptions();
			opt.setSavePrettyPrint();
			String xmlMetsContent = metsDoc.xmlText(opt);
			FileUtil.writeFile(ieXML, xmlMetsContent);

			//validate against mets_rosetta.xsd

			//Need to replace manually the namespace with Rosetta Mets schema in order to pass validation against mets_rosetta.xsd
			String xmlRosettaMetsContent = xmlMetsContent.replaceAll(XML_SCHEMA, XML_SCHEMA_REPLACEMENT);
			xmlRosettaMetsContent = xmlMetsContent.replaceAll(METS_SCHEMA, ROSETTA_METS_SCHEMA);

			validateXML(ieXML.getAbsolutePath(), xmlRosettaMetsContent, ROSETTA_METS_XSD);

//			void connect
			
			System.out.println("vorzeitiges Ende");
			if(true) return;

			// 3. Place the SIP directory in a folder that can be accessed by the Rosetta application (using FTP is a valid approach)
			ProducerWebServices producerWebServices = new ProducerWebServices_Service(new URL(PRODUCER_WSDL_URL),
					new QName("http://dps.exlibris.com/", "ProducerWebServices")).getProducerWebServicesPort();
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

		} catch (Exception e) {
			e.printStackTrace();
			return;
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