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
//import com.exlibris.repository.persistence.sip.HsSipItem.SipItemStatus;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.userauth.keyprovider.KeyProvider;
import com.zbmed.sftp.SFTPManager;

public class MetsIngester {

	static final String fs = System.getProperty("file.separator");
	public static final String sipPath = System.getProperty("user.home").concat(fs).concat("workspace").concat(fs)
			.concat("metsSIPs").concat(fs);

	private static final String sshKeyPath = System.getProperty("user.home").concat(fs).concat("SubApp.ppk");

	private static final String userName = "SubApp ZB MED";
	private static final String institution = "ZBM";
	private static String password = "";
	private static final String depositSetId = "";//TODO welchen Set wollen wir? 54429706
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
				String rosettaInstance = null;
				if (instName.equals("dev")) {
					rosettaURL = "https://rosetta.develop.lza.tib.eu";
					rosettaInstance = "dev";
				} else if (instName.equals("test")) {
					rosettaURL = "https://rosetta.test.lza.tib.eu";
					rosettaInstance = "test";
				} else if (instName.equals("prod")) {
					rosettaURL = "https://rosetta.lza.tib.eu";
					rosettaInstance = "prod";
				} else {
					continue;
				}
				File[] matIds = inst.listFiles();
				for (File matId : matIds) {
					String materialflowId = matId.getName();
					File[] prodIDs = matId.listFiles();
					for (File prodID : prodIDs) {
						String producerId = prodID.getName();
						File[] sips = prodID.listFiles();
						for (File sip : sips) {
							String subDirectoryName = sip.getAbsolutePath().concat("/");
							ingest(subDirectoryName, rosettaURL, rosettaInstance, materialflowId, producerId);
						}
					}
				}
			}
		}
	}

	public static void ingest(String subDirectoryName, String rosettaURL, String rosettaInstance, String materialflowId, String producerId)
			throws Exception {
		final String DEPOSIT_WSDL_URL = rosettaURL.concat("/dpsws/deposit/DepositWebServices?wsdl");
		final String PRODUCER_WSDL_URL = rosettaURL.concat("/dpsws/backoffice/ProducerWebServices?wsdl");
		String SIP_STATUS_WSDL_URL = rosettaURL.concat("/dpsws/repository/SipWebServices?wsdl");

		System.out.println("ingeste: '".concat(subDirectoryName).concat("' in '").concat(rosettaURL).concat("' unter '")
				.concat(materialflowId).concat("'."));

		// 3. Place the SIP directory in a folder that can be accessed by the Rosetta application (using FTP is a valid approach)
		SSHClient ssh = new SSHClient();
		ssh.loadKnownHosts();
		ssh.connect("transfer.lza.tib.eu");
		String sipId = subDirectoryName.substring(subDirectoryName.lastIndexOf("/", subDirectoryName.length()-2) + 1, subDirectoryName.length()-1);
		final String SubAppPath = "/exchange/lza/lza-zbmed/".concat(rosettaInstance).concat("/SubApp/");
		final String sipPathTib = SubAppPath.concat(sipId).concat("/");
		try {
			KeyProvider keys = ssh.loadKeys(sshKeyPath);
			ssh.authPublickey("lza-zbmed", keys);
			final SFTPClient sftp = ssh.newSFTPClient();
			SFTPManager.put(sftp, subDirectoryName, sipPathTib);
			sftp.chmod(sipPathTib, 509);//Decimal conversion of octal 0755
		} finally {
			ssh.disconnect();
		}
		ssh.close();

		//submit
		// 4. Set Authentication Header on service
		if (password.contentEquals("")) {
			String propertyDateiPfad = System.getProperty("user.home").concat(fs).concat("Rosetta_Properties.txt");
			Properties_Manager prop = new Properties_Manager(propertyDateiPfad);
			password = prop.readStringFromProperty("SubApp_Password");
		}

		DepositWebServices_Service depWS = new DepositWebServices_Service(new URL(DEPOSIT_WSDL_URL),
				new QName("http://dps.exlibris.com/", "DepositWebServices"));
		depWS.setHandlerResolver(new HeaderHandlerResolver(userName, password, institution));

		// 5. Submit Deposit
		String retval = depWS.getDepositWebServicesPort().submitDepositActivity(null, materialflowId, sipId+"/",
				producerId, depositSetId);
		System.out.println("Submit Deposit Result: " + retval);

		DepositResultDocument depositResultDocument = DepositResultDocument.Factory.parse(retval);
		DepositResult depositResult = depositResultDocument.getDepositResult();

		// 6.check status of sip when deposit was successful
		for (int i = 0; i < 100; i++) {
			Thread.sleep(1000);//wait until deposit is in
			if (depositResult.getIsError()) {
				System.out.println("Submit Deposit Failed");
				break;
			} else {
				SipStatusInfo status = new SipWebServices_Service(new URL(SIP_STATUS_WSDL_URL),
						new QName("http://dps.exlibris.com/", "SipWebServices")).getSipWebServicesPort()
								.getSIPStatusInfo(String.valueOf(depositResult.getSipId()));
				String statusString = status.getStatus();
				System.out.println("Submitted Deposit Status: " + statusString);
				System.out.println("Submitted Deposit Stage: " + status.getStage());
				System.out.println("Submitted Deposit is in Module: " + status.getModule());
				if (statusString.equals("IN_TA") || statusString.equals("dummy")) {
					break;
				}
			}
		}
		ssh = new SSHClient();
		ssh.loadKnownHosts();
		ssh.connect("transfer.lza.tib.eu");
		try {
			KeyProvider keys = ssh.loadKeys(sshKeyPath);
			ssh.authPublickey("lza-zbmed", keys);
			final SFTPClient sftp = ssh.newSFTPClient();
			SFTPManager.rmDir(sftp, sipPathTib);
		} finally {
			ssh.disconnect();
		}
		ssh.close();
	}
}
