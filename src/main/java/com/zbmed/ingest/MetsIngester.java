package com.zbmed.ingest;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import javax.xml.namespace.QName;
import com.exlibris.digitool.deposit.service.xmlbeans.DepositResultDocument;
import com.exlibris.digitool.deposit.service.xmlbeans.DepositResultDocument.DepositResult;
import com.exlibris.dps.DepositWebServices_Service;
import com.exlibris.dps.SipStatusInfo;
import com.exlibris.dps.SipWebServices_Service;
import com.exlibris.dps.sdk.pds.HeaderHandlerResolver;
//import com.exlibris.repository.persistence.sip.HsSipItem.SipItemStatus;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.userauth.keyprovider.KeyProvider;
import com.zbmed.sftp.SFTPManager;
import com.zbmed.utilities.StatusFile;

public class MetsIngester {

	static final String fs = System.getProperty("file.separator");

	private static final int sipsToGo = -1;

	public static final String sipPath = System.getProperty("user.home").concat(fs).concat("workspace").concat(fs)
			.concat("metsSIPs").concat(fs);

	private static final String sshKeyPath = System.getProperty("user.home").concat(fs).concat("SubApp.ppk");

	private static final String userName = "SubApp ZB MED";
	//	private static final String userName = "SubAppZBMED";
	//	private static final String userName = "wutschkaa";
	private static String institution = "";
	private static String password = "";
	private static final String depositSetId = "";//TODO welchen Set wollen wir? 54429706
	//	private static final String METS_XSD = "mets.xsd";

	private static StatusFile statusFile;

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
					institution = "ZBM";
				} else if (instName.equals("test")) {
					rosettaURL = "https://rosetta.test.lza.tib.eu";
					rosettaInstance = "test";
					institution = "ZBMED";
				} else if (instName.equals("prod")) {
					rosettaURL = "https://rosetta.lza.tib.eu";
					rosettaInstance = "prod";
					institution = "ZBMED";
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
						int sipCount = 0;
						for (File sip : sips) {
							if (sipCount == sipsToGo) {
								break;
							}
							String subDirectoryName = sip.getAbsolutePath().concat("/");
							statusFile = new StatusFile(new File(subDirectoryName));
							if (!statusFile.exists()) {
								System.out.println("#".concat(Integer.toString(sipCount + 1)).concat(" ingeste: '")
										.concat(subDirectoryName).concat("' in '").concat(rosettaURL).concat("'."));
								ingest(subDirectoryName, rosettaURL, rosettaInstance, materialflowId, producerId);
							} else {
								System.out.println("#".concat(Integer.toString(sipCount + 1)).concat(" SIP: '")
										.concat(subDirectoryName).concat("' wurde bereits behandelt. Status: '")
										.concat(statusFile.status).concat("'."));
							}
							++sipCount;
						}
					}
				}
			}
		}
	}

	public static void ingest(String subDirectoryName, String rosettaURL, String rosettaInstance, String materialflowId,
			String producerId) throws Exception {
		final String DEPOSIT_WSDL_URL = rosettaURL.concat("/dpsws/deposit/DepositWebServices?wsdl");
		final String PRODUCER_WSDL_URL = rosettaURL.concat("/dpsws/backoffice/ProducerWebServices?wsdl");
		final String SIP_STATUS_WSDL_URL = rosettaURL.concat("/dpsws/repository/SipWebServices?wsdl");

		// 3. Place the SIP directory in a folder that can be accessed by the Rosetta application (using FTP is a valid approach)
		String sipId = subDirectoryName.substring(subDirectoryName.lastIndexOf("/", subDirectoryName.length() - 2) + 1,
				subDirectoryName.length() - 1);
		final String SubAppPath = "/exchange/lza/lza-zbmed/".concat(rosettaInstance).concat("/SubApp/");
		final String sipPathTib = SubAppPath.concat(sipId).concat("/");
		upload2Transferserver(subDirectoryName, sipPathTib, rosettaInstance);
		//		System.out.println("SIP auf Transferserver übertragen");

		//submit
		// 4. Set Authentication Header on service
		if (password.contentEquals("")) {
			String propertyDateiPfad = System.getProperty("user.home").concat(fs).concat("Rosetta_Properties.txt");
			PropertiesManager prop = new PropertiesManager(propertyDateiPfad);
			password = prop.readStringFromProperty("SubApp_Passwort");
			//			password = prop.readStringFromProperty("Wutschka_dev_Passwort");
		}

		//		System.out.println(DEPOSIT_WSDL_URL);
		//		System.out.println("Username = '" + userName + "' Passwort = '" + password + "' Institution = '" + institution + "'");

		DepositWebServices_Service depWS = new DepositWebServices_Service(new URL(DEPOSIT_WSDL_URL),
				new QName("http://dps.exlibris.com/", "DepositWebServices"));
		depWS.setHandlerResolver(new HeaderHandlerResolver(userName, password, institution));

		// 5. Submit Deposit
		System.out.println("Submit Deposit...");
		Boolean repeat = true;
		String retval = null;
		while (repeat) {
			try {
				retval = depWS.getDepositWebServicesPort().submitDepositActivity(null, materialflowId, sipId + "/",
						producerId, depositSetId);
				repeat = false;
			} catch (Exception e) {
				System.err.println("Submit Deposit fehlgeschlagen");
				repeat = true;
				Thread.sleep(1000);
			}
		}
		if (retval == null) {
			System.err.println("Submit Deposit ergab Nullantwort");
			throw new Exception();
		}
		System.out.println("Submit Deposit Result: '" + retval + "' für materialflowId: '" + materialflowId
				+ "', sipId: '" + sipId + "/', producerId: '" + producerId + "' und depositSetId: '" + depositSetId + "'.");

		DepositResultDocument depositResultDocument = DepositResultDocument.Factory.parse(retval);
		DepositResult depositResult = depositResultDocument.getDepositResult();

		// 6.check status of sip when deposit was successful
		if (depositResult.getIsError()) {
			System.err.println("Submit Deposit Failed");
			statusFile.create("ERROR");
			saveErrorInfos(depositResult);
		} else {
			int waited = 0;
			SipStatusInfo lastStatus = null, status = null;
			while(true) {
				Thread.sleep(1000);
				++waited;
				try {
					status = new SipWebServices_Service(new URL(SIP_STATUS_WSDL_URL),
							new QName("http://dps.exlibris.com/", "SipWebServices")).getSipWebServicesPort()
									.getSIPStatusInfo(String.valueOf(depositResult.getSipId()), false);
					String statusString = status.getStatus();
					if (!compareStatus(status, lastStatus)) {
						System.out.println("Nach " + waited + " Sekunden: " + "Submitted Deposit Status: " + statusString
								+ "; Submitted Deposit Stage: " + status.getStage() + "; Submitted Deposit is in Module: "
								+ status.getModule());
					}
					if (statusString.equals("IN_TA") || statusString.equals("IN_HUMAN_STAGE")
							|| statusString.equals("FINISHED")) {
						statusFile.create("DONE");
						saveDoneInfos(status, waited, depositResult);
						break;
					}
					lastStatus = status;
				} catch (Exception e) {
					System.err.println("Statusabfrage fehlgeschlagen");
					status = lastStatus;
				}
				if (waited >= 10800) { //3 Stunden reine Wartezeit
					System.err.println("Zu oft nachgefragt, breche alles ab.");
					throw new Exception();
				}
			}
		}
		deleteFromTransferserver(sipPathTib);
	}

	private static boolean compareStatus(SipStatusInfo status1, SipStatusInfo status2) {
		if (status1 == null && status2 == null)
			return true;
		if (status1 == null)
			return false;
		if (status2 == null)
			return false;
		String str1 = status1.getStatus();
		String str2 = status2.getStatus();
		if (str1 == null && str2 != null)
			return false;
		if (str1 != null && str2 == null)
			return false;
		if (str1 != null && str2 != null && !str1.equals(str2))
			return false;
		str1 = status1.getStage();
		str2 = status2.getStage();
		if (str1 == null && str2 != null)
			return false;
		if (str1 != null && str2 == null)
			return false;
		if (str1 != null && str2 != null && !str1.equals(str2))
			return false;
		str1 = status1.getModule();
		str2 = status2.getModule();
		if (str1 == null && str2 != null)
			return false;
		if (str1 != null && str2 == null)
			return false;
		if (str1 != null && str2 != null && !str1.equals(str2))
			return false;
		return true;
	}

	private static void saveDoneInfos(SipStatusInfo status, int waited, DepositResult depositResult) throws Exception {
		statusFile.saveStringToProperty("SekundenGebraucht", Integer.toString(waited));
		statusFile.saveStringToProperty("Status", status.getStatus());
		statusFile.saveStringToProperty("Stage", status.getStage());
		statusFile.saveStringToProperty("Module", status.getModule());
		statusFile.saveStringToProperty("Error", status.getError());
		statusFile.saveStringToProperty("ExternalId", status.getExternalId());
		statusFile.saveStringToProperty("ExternalSystem", status.getExternalSystem());
		statusFile.saveStringToProperty("IePids", status.getIePids());
		statusFile.saveStringToProperty("SipId", status.getSipId());
		statusFile.saveStringToProperty("SipStatusInfo", status.toString());
		statusFile.saveStringToProperty("sipID", String.valueOf(depositResult.getSipId()));
		statusFile.saveStringToProperty("CreationDate", depositResult.getCreationDate());
		statusFile.saveStringToProperty("MessageCode", depositResult.getMessageCode());
		statusFile.saveStringToProperty("MessageDesc", depositResult.getMessageDesc());
		statusFile.saveStringToProperty("UserParams", depositResult.getUserParams());
		statusFile.saveStringToProperty("XmlText", depositResult.xmlText());
		statusFile.saveStringToProperty("DepositResult", depositResult.toString());
	}

	private static void saveErrorInfos(DepositResult depositResult) throws Exception {
		statusFile.saveStringToProperty("sipID", String.valueOf(depositResult.getSipId()));
		statusFile.saveStringToProperty("CreationDate", depositResult.getCreationDate());
		statusFile.saveStringToProperty("MessageCode", depositResult.getMessageCode());
		statusFile.saveStringToProperty("MessageDesc", depositResult.getMessageDesc());
		statusFile.saveStringToProperty("UserParams", depositResult.getUserParams());
		statusFile.saveStringToProperty("XmlText", depositResult.xmlText());
		statusFile.saveStringToProperty("DepositResult", depositResult.toString());
	}

	private static void upload2Transferserver(String from, String to, String rosettaInstance) throws Exception {
		boolean done = false;
		while (!done) {
			try {
				SSHClient ssh = new SSHClient();
				ssh.loadKnownHosts();
				ssh.connect("transfer.lza.tib.eu");
				try {
					KeyProvider keys = ssh.loadKeys(sshKeyPath);
					ssh.authPublickey("lza-zbmed", keys);
					final SFTPClient sftp = ssh.newSFTPClient();
					SFTPManager.put(sftp, from, to);
					sftp.chmod(to, 509);//Decimal conversion of octal 0755
					done = true;
				} finally {
					ssh.disconnect();
				}
				ssh.close();
			} catch (Exception e) {
				System.err.println("Fehler beim Hochladen.");
				Thread.sleep(1000);
				if (folderExists(to))
					deleteFromTransferserver(to);
			}
		}
	}

	private static void deleteFromTransferserver(String folder) throws Exception {//noch unzuverlässig
		boolean done = false;
		while (!done) {
			try {
				SSHClient ssh = new SSHClient();
				ssh.loadKnownHosts();
				ssh.connect("transfer.lza.tib.eu");//könnte fehlschlagen
				try {
					KeyProvider keys = ssh.loadKeys(sshKeyPath);
					ssh.authPublickey("lza-zbmed", keys);
					final SFTPClient sftp = ssh.newSFTPClient();
					SFTPManager.rmDir(sftp, folder);
				} finally {
					ssh.disconnect();
				}
				ssh.close();
				done = true;
			} catch (Exception e) {
				System.err.println("Fehler beim löschen.");
				Thread.sleep(1000);
				done = !folderExists(folder);
			}
		}
	}

	private static boolean folderExists(String folder) {
		boolean ret = false, done = false;
		int cut = folder.lastIndexOf("/", folder.length() - 2);
		String subfolder = folder.substring(0, cut);
		String folderName = folder.substring(cut);
		while (!done) {
			try {
				SSHClient ssh = new SSHClient();
				ssh.loadKnownHosts();
				ssh.connect("transfer.lza.tib.eu");
				try {
					KeyProvider keys = ssh.loadKeys(sshKeyPath);
					ssh.authPublickey("lza-zbmed", keys);
					final SFTPClient sftp = ssh.newSFTPClient();
					System.out.println("'" + subfolder + "' '" + folderName + "'");
					ret = SFTPManager.exists(sftp, subfolder, folderName);
					done = true;
				} finally {
					System.err.println("Fehler bei Abfrage ob Ordner existiert.");
					ssh.disconnect();
				}
				ssh.close();
			} catch (Exception e) {
				System.err.println("Fehler 2 bei Abfrage ob Ordner existiert.");
			}
		}
		return ret;
	}
}
