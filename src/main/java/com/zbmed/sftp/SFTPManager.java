package com.zbmed.sftp;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import com.zbmed.utilities.Utilities;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.RemoteResourceInfo;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.userauth.keyprovider.KeyProvider;
import net.schmizz.sshj.xfer.FilePermission;

public class SFTPManager {
	private static final String fs = Utilities.fs;
	private static final String folder_on_working_machine = System.getProperty("user.home").concat(fs)
			.concat("workspace");
	private static final String subDirectoryName = folder_on_working_machine.concat(fs).concat("DepositExample4");
	private static final String sshKeyPath = System.getProperty("user.home").concat(fs).concat("SubApp.ppk");

	public static void printPerms(SFTPClient sftp, String path) throws IOException {
		if (path.endsWith("/")) {
			path = path.substring(0, path.length()-1);
		}
		System.out.println("Permissions of ".concat(path).concat("/"));
		Set<FilePermission> permSet = sftp.perms(path);
		for (FilePermission permItem : permSet) {
			System.out.println(permItem.toString());
		}
	}

	public static boolean exists(SFTPClient sftp, String path, String thing) throws IOException {
		List<RemoteResourceInfo> lsRet = sftp.ls(path);
		for (RemoteResourceInfo lsitem : lsRet) {
			if (lsitem.getName().equals(thing)) {
				return true;
			}
		}
		return false;
	}
	
	public static void printLs(SFTPClient sftp, String path) throws IOException {
		System.out.println("Content of ".concat(path));
		List<RemoteResourceInfo> lsRet = sftp.ls(path);
		for (RemoteResourceInfo lsitem : lsRet) {
			System.out.println("* ".concat(lsitem.getName()));
		}
	}

	public static void put(SFTPClient sftp, String from, String to, boolean verbose) throws IOException {
		if (verbose)
			System.out.println("Uploading from: ".concat(from).concat(" to: ").concat(to));
		sftp.put(from, to);
	}

	public static void put(SFTPClient sftp, String from, String to) throws IOException {
		put(sftp, from, to, false);
	}

	public static void rmDir(SFTPClient sftp, String path, boolean verbose) throws IOException {
		if (verbose)
			System.out.println("Deleting: ".concat(path));
		List<RemoteResourceInfo> lsRet = sftp.ls(path);
		for (RemoteResourceInfo lsitem : lsRet) {
			if (lsitem.isRegularFile()) {
				sftp.rm(lsitem.getPath());
			} else {
				rmDir(sftp, lsitem.getPath(), false);
			}
		}
		sftp.rmdir(path);
	}

	public static void rmDir(SFTPClient sftp, String path) throws IOException {
		rmDir(sftp, path, false);
	}
	
	public static void mkDir(SFTPClient sftp, String dirname) throws IOException {
		sftp.mkdir(dirname);
	}

	public static void main(String[] args) throws IOException {
		final SSHClient ssh = new SSHClient();
		ssh.loadKnownHosts();
		ssh.connect("transfer.lza.tib.eu");
		try {
			KeyProvider keys = ssh.loadKeys(sshKeyPath);
			ssh.authPublickey("lza-zbmed", keys);
			final SFTPClient sftp = ssh.newSFTPClient();
			final String GMSPath = "/exchange/lza/lza-zbmed/dev/gms/";
			String SIPPath;
			//			SIPPath = GMSPath.concat("DepositExample3/");
			//			printLs(sftp, SIPPath);
			//			printPerms(sftp, SIPPath);
			SIPPath = GMSPath.concat("DepositExample4/");
			put(sftp, subDirectoryName, SIPPath, true);
			sftp.chmod(SIPPath, 509);//Decimal conversion of octal 0755
			printLs(sftp, SIPPath);
			printPerms(sftp, SIPPath);
			rmDir(sftp, SIPPath, true);
		} finally {
			ssh.disconnect();
		}
		ssh.close();
	}
}
