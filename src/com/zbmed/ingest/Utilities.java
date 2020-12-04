package com.zbmed.ingest;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Properties;

public class Utilities {
	public final static String fs = System.getProperty("file.separator");
	
	public static String readline() throws IOException {
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		String ausgelesen = in.readLine();
		return ausgelesen;
	}

	public static void saveStringToProperty(String ziel, String prop, String value) throws IOException {
		Writer writer = new FileWriter(ziel);
		Properties properties = new Properties();
		properties.setProperty(prop, value);
		properties.store(writer, "default Kommentar");
		writer.close();
	}

	public static String readStringFromProperty(String quelle, String prop) throws IOException {
		Properties properties = new Properties();
		BufferedInputStream reader = new BufferedInputStream(new FileInputStream(quelle));
		properties.load(reader);
		reader.close();
		return properties.getProperty(prop);
	}

	public static void deleteDir(String path) {
		File filePath = new File(path);
		if (filePath.exists()) deleteDir(filePath);
	}

	public static void deleteFile(String filePath) {
		try {
			File file = new File(filePath);
			if(file.exists()) {
				file.delete();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void deleteDir(File path) {
		for (File file : path.listFiles()) {
			if (file.isDirectory())
				deleteDir(file);
			file.delete();
		}
		path.delete();
	}
}
