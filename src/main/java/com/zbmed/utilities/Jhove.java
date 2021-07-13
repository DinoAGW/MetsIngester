package com.zbmed.utilities;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

public class Jhove {

	public static String jhove(String module, String file) throws IOException {
		String jhoveCmd = System.getProperty("user.home") + "/jhove/jhove";
		String[] jhoveParams = new String[] { jhoveCmd, "-m", module, file };
		ProcessBuilder pb = new ProcessBuilder(jhoveParams);
		pb.redirectErrorStream(true);
		Process process = pb.start();
		BufferedReader inStreamReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
		String line = "";
		String lines = "";
		while ((line = inStreamReader.readLine()) != null) {
			lines = lines.concat(line).concat("\n");
		}
		return lines;
	}

	public static String status(String lines) throws Exception {
		String search = "  Status: ";
		for (String line : lines.split("\n")) {
			if (line.startsWith(search)) {
				return line.substring(search.length());
			}
		}
		System.err.println("Keine Zeile beginnt mit '" + search + "'");
		throw new Exception();
	}

	public static String errorMessage(String lines) throws Exception {
		String search = "  ErrorMessage: ";
		for (String line : lines.split("\n")) {
			if (line.startsWith(search)) {
				return line.substring(search.length());
			}
		}
		return "No ErrorMessage";
	}

	public static boolean wellFormedAndValid(String lines) throws Exception {
		return (status(lines).equals("Well-Formed and valid") || status(lines).equals("Well-Formed"));
	}

	public static void printJhove(String module, File file) throws Exception {
		String lines = jhove(module, file.getAbsolutePath());
		//		System.out.println(lines);
		String ausgabe = "jhove -m " + module + " " + file;
		System.out.println(ausgabe + "; Status: " + status(lines));
		if (wellFormedAndValid(lines)) {
			System.out.println("alles ok");
		} else {
			System.out.println("Fehlermeldung: " + errorMessage(lines));
		}
	}

	/*
	 * validiere alle xml und pdf Dateien eines Ordners und Unterordners
	 */
	public static void printJhove(File file) throws Exception {
		if (file.isDirectory()) {
			for (File inFile : file.listFiles()) {
				printJhove(inFile);
			}
		} else if (file.getName().endsWith(".xml")) {
			printJhove("XML-hul", file);
		} else if (file.getName().endsWith(".pdf")) {
			printJhove("PDF-hul", file);
		}
	}

	public static void main(String[] args) throws Exception {
//		String file = System.getProperty("user.home")
//				+ "/workspace/metsSIPs/dev/76661659/ri2009/content/streams/SOURCE_MD/SRU.xml";
		String filesRootFolder = System.getProperty("user.home") + "/workspace/metsSIPs/";
		printJhove(new File(filesRootFolder));
		System.out.println("Jhove.main() Ende");
	}
}
