package com.zbmed.ingest;

import java.io.File;
import java.net.URL;
import javax.xml.namespace.QName;
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
	
	static final String fs = System.getProperty("file.separator");
	public static final String sipPath = System.getProperty("user.home").concat(fs).concat("workspace").concat(fs)
			.concat("metsSIPs").concat(fs);
	
	private static final String userName = "SubApp ZB MED";
	private static final String institution = "ZBM";
	private static String password = "";
	private static final String depositSetId = "2049290";//TODO was könnte das sein? vielleicht Producer "ZB MED - German Medical Science" mit der ID 2049290?
	//	private static final String METS_XSD = "mets.xsd";
	
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
		
		System.out.println("ingeste: '".concat(subDirectoryName).concat("' in '").concat("rosettaURL").concat("' unter '").concat(materialflowId).concat("'."));

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
}
