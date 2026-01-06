package de.zbmed.ingest;

import java.io.File;
import java.net.URL;

import javax.xml.namespace.QName;

import com.exlibris.dps.SipStatusInfo;
import com.exlibris.dps.SipWebServices_Service;

import de.zbmed.utilities.StatusFile;

public class SipChecker {
	static final String fs = System.getProperty("file.separator");
	public static final String sipPath = System.getProperty("user.home").concat(fs).concat("workspace").concat(fs)
			.concat("metsSIPs").concat(fs);

	public static void checkSIPsFromJC2018() throws Exception {
		String jcPath = System.getProperty("user.home").concat(fs).concat("workspace").concat(fs).concat("metsSIPs")
				.concat(fs).concat("JC2018").concat(fs);
		File jcPathFile = new File(jcPath);
		File[] sips = jcPathFile.listFiles();
		
		int anzahl = 0;
		int regulaere = 0;
		int neuer = 0;
		int gefunden = 0;
		for (File sip : sips) {
			StatusFile statusFile = new StatusFile(sip);
			PropertiesManager prop = new PropertiesManager(sip.getAbsolutePath().concat(fs).concat("DONE.txt"));
			String sipId_str;
			try {
				sipId_str = prop.readStringFromProperty("sipID");
			} catch (Exception e) {
				continue;
			}
			int sipId = Integer.parseInt(sipId_str);
			boolean info = false;
			if (!info)
				System.out.println("SIP '" + sip.getAbsolutePath() + "' ID '" + sipId_str + "'.");
			++anzahl;
		}
		System.out.println("Geprüft: " + anzahl + " Regulär: " + regulaere + " Neuer: " + neuer + " zusammen: "
				+ (regulaere + neuer) + ".");
		System.out.println("Gefunden: " + gefunden);
	}

	public static void showNotDones() throws Exception {
		File sipPathFile = new File(sipPath);
		File[] insts = sipPathFile.listFiles();
		if (insts == null) {
			System.err.println("Keine Unterordner unter '".concat(sipPath).concat("' gefunden."));
			throw new Exception();
		}
		int errorSips = 0;
		int doneSips = 0;
		int restSips = 0;
		int nasSips = 0;
		int minSipId = 0;
		int maxSipId = 0;
		for (File inst : insts) {
			if (inst.isDirectory()) {
				String instName = inst.getName();
				String rosettaInstance = null;
				if (instName.equals("dev")) {
					rosettaInstance = "dev";
				} else if (instName.equals("test")) {
					rosettaInstance = "test";
				} else if (instName.equals("prod")) {
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
							StatusFile statusFile = new StatusFile(new File(subDirectoryName));
							if (!statusFile.exists()) {
								++nasSips;
								System.out.println(
										"Ohne: '".concat(subDirectoryName).concat("' in '").concat(rosettaInstance).concat("'."));
							} else {
								if (statusFile.status.contentEquals("ERROR")) {
									++errorSips;
									System.err.println("Error: '".concat(subDirectoryName).concat("' in '")
											.concat(rosettaInstance).concat("'."));
								} else if (statusFile.status.contentEquals("DONE")) {
									++doneSips;
									PropertiesManager prop = new PropertiesManager(subDirectoryName.concat("DONE.txt"));
									String sipId_str = prop.readStringFromProperty("sipID");
									int sipId = Integer.valueOf(sipId_str);
									if (minSipId > sipId || minSipId == 0)
										minSipId = sipId;
									if (maxSipId < sipId || maxSipId == 0)
										maxSipId = sipId;
									System.out.println("Done: '".concat(subDirectoryName).concat("' in '")
											.concat(rosettaInstance).concat("' mit sipId '").concat(sipId_str).concat("'."));
								} else {
									++restSips;
									System.out.println("Rest: '".concat(subDirectoryName).concat("' in '")
											.concat(rosettaInstance).concat("'."));
								}
							}
						}
					}
				}
			}
		}
		System.out.println("Insgesamt " + doneSips + " SIPs mit Status DONE");
		System.out.println("Insgesamt " + errorSips + " SIPs mit Status ERROR");
		System.out.println("Insgesamt " + nasSips + " SIPs ohne Status");
		System.out.println("Insgesamt " + restSips + " SIPs mit anderem Status");
		System.out.println("SipIDs reichen von " + minSipId + " bis " + maxSipId + " .");
	}

	public static void main(String[] args) throws Exception {
		//		showNotDones();
		checkSIPsFromJC2018();
	}

}
