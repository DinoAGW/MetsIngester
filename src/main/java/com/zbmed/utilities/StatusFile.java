package com.zbmed.utilities;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Properties;

public class StatusFile {
	private static String fs = System.getProperty("file.separator");

	public File folder;
	public File file;
	public String status;
	Properties properties;

	public StatusFile(File folder) throws Exception {
		this.folder = folder;
		if (!folder.exists()) {
			System.err.println("Ordner '".concat(folder.toString().concat(fs)).concat("' existiert nicht"));
			throw new Exception();
		}
		if (!folder.isDirectory()) {
			System.err.println("Datei '".concat(folder.toString()).concat("' ist kein Ordner"));
			throw new Exception();
		}
		File doneFile = new File(folder.getAbsolutePath().concat(fs).concat("DONE.txt"));
		File errorFile = new File(folder.getAbsolutePath().concat(fs).concat("ERROR.txt"));
		File abortFile = new File(folder.getAbsolutePath().concat(fs).concat("ABORT.txt"));
		int count = 0;
		this.file = null;
		if (doneFile.exists()) {
			++count;
			this.file = doneFile;
			this.status = "DONE";
		}
		if (errorFile.exists()) {
			++count;
			this.file = errorFile;
			this.status = "ERROR";
		}
		if (abortFile.exists()) {
			++count;
			this.file = abortFile;
			this.status = "ABORT";
		}
		if (count > 1) {
			System.err.println("Es darf nur maximal eine Art StatusDatei geben.");
			throw new Exception();
		}
		if (count == 1) {
			BufferedInputStream reader = new BufferedInputStream(new FileInputStream(this.file));
			this.properties = new Properties();
			this.properties.load(reader);
			reader.close();
		}
	}

	public boolean exists() {
		return (this.file != null && this.file.exists());
	}

	public boolean create(String sorte) throws Exception {
		if (exists()) {
			System.err.println("Es existiert bereits eine StatusDatei.");
			throw new Exception();
		}
		if (!(sorte.equalsIgnoreCase("DONE") || sorte.equalsIgnoreCase("ERROR") || sorte.equalsIgnoreCase("ABORT"))) {
			System.err.println("Sorte '".concat(sorte).concat("' ist weder DONE, noch ERROR, noch ABORT."));
		}
		String datei = this.folder.toString().concat(fs).concat(sorte.toUpperCase()).concat(".txt");
		this.file = new File(datei);
		this.status = sorte.toUpperCase();
		this.properties = new Properties();
		return this.file.createNewFile();
	}

	public void remove() throws Exception {
		if (!exists()) {
			System.err.println("Es existiert keine StatusDatei.");
			throw new Exception();
		}
		this.file.delete();
		this.file = null;
		this.status = null;
		this.properties = null;
	}

	public void saveStringToProperty(String prop, String value) throws Exception {
		if (!this.exists()) {
			System.err.println("Es existiert keine StatusDatei.");
			throw new Exception();
		}
		Writer writer = new FileWriter(this.file);
		if (value==null) {
			value = "null";
		}
		try {
			this.properties.setProperty(prop, value);
		} catch (Exception e) {
			System.err.println("Konnte nicht '" + prop + "' auf '" + value + "' setzen.");
			e.printStackTrace();
		}
		this.properties.store(writer, "default Kommentar");
		writer.close();
	}

	public String readStringFromProperty(String prop) throws IOException {
		BufferedInputStream reader = new BufferedInputStream(new FileInputStream(file));
		this.properties.load(reader);
		reader.close();
		return this.properties.getProperty(prop);
	}

	public static void main(String[] args) throws Exception {
		String path = System.getProperty("user.home").concat(fs).concat("workspace").concat(fs);
		StatusFile test = new StatusFile(new File(path));
		System.out.println(test.folder);
		System.out.println("Existiert ein StatusFile? " + test.exists());
		
		test.create("done");System.out.println("Existiert ein StatusFile? " + test.exists());
		test.saveStringToProperty("test", "erfolgreich");test.saveStringToProperty("test2", "auch erfolgreich");
		System.out.println(test.readStringFromProperty("test"));System.out.println(test.readStringFromProperty("test2"));
		test.remove();System.out.println("Existiert ein StatusFile? " + test.exists());
	}
}
