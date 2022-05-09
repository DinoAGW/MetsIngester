package com.zbmed.werkstatt;

import java.io.IOException;
import java.net.URL;

import javax.xml.namespace.QName;

import com.exlibris.dps.DepositWebServices_Service;
import com.exlibris.dps.sdk.pds.HeaderHandlerResolver;
import com.exlibris.dps.sdk.pds.PdsClient;
import com.zbmed.ingest.PropertiesManager;

public class CheckMets {
	static final String fs = System.getProperty("file.separator");
	private static final String sshKeyPath = System.getProperty("user.home").concat(fs).concat("SubApp.ppk");
	
	private static final String userName = "SubApp ZB MED";
	private static String institution = "";
	private static String password = "";
	private static String instName = "dev";

	public static void main(String[] args) throws IOException {
		String rosettaURL = null;
		String rosettaInstance = null;
		if (instName.equals("dev")) {
			rosettaURL = "https://rosetta.develop.lza.tib.eu";
			rosettaInstance = "dev";
			institution = "ZBM";
		} else if (instName.equals("test")) {
			rosettaURL = "https://rosetta.test.lza.tib.eu";
			rosettaInstance = "test";
			institution = "ZBMED";
		} else if (instName.equals("prod")) {
			rosettaURL = "https://rosetta.lza.tib.eu";
			rosettaInstance = "prod";
			institution = "ZBMED";
		}
		String materialflowId = null;
		String producerId = null;
		
		final String DEPOSIT_WSDL_URL = rosettaURL.concat("/dpsws/deposit/DepositWebServices?wsdl");
		final String PRODUCER_WSDL_URL = rosettaURL.concat("/dpsws/backoffice/ProducerWebServices?wsdl");
		final String SIP_STATUS_WSDL_URL = rosettaURL.concat("/dpsws/repository/SipWebServices?wsdl");
		final String IE_WSDL_URL = rosettaURL.concat("/dpsws/repository/IEWebServices?wsdl");
		
//		final String PDS_URL =  
		
		if (password.contentEquals("")) {
			String propertyDateiPfad = System.getProperty("user.home").concat(fs).concat("Rosetta_Properties.txt");
			PropertiesManager prop = new PropertiesManager(propertyDateiPfad);
			password = prop.readStringFromProperty("SubApp_Passwort");
		}
		
//		PdsClient pds = PdsClient.getInstance();
//		pds.init(PDS_URL, false);
		
//		DepositWebServices_Service depWS = new DepositWebServices_Service(new URL(DEPOSIT_WSDL_URL),
//				new QName("http://dps.exlibris.com/", "DepositWebServices"));
//		depWS.setHandlerResolver(new HeaderHandlerResolver(userName, password, institution));
//		
//		depWS.getDepositWebServicesPort().
	}

}
