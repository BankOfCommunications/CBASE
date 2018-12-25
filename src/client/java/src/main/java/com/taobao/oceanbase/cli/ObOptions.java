package com.taobao.oceanbase.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

public class ObOptions {

	private static final String HOST = "host";
	private static final String PORT = "port";

	private String host = null;
	private int port;

	private static Options options = null;
	private CommandLine cmd = null;

	static {
		options = new Options();
		options.addOption(HOST, true, "rootserver's host address");
		options.addOption(PORT, true, "rootserver's listen port");
	};

	private static void printUsage() {
		System.err.println("");
		System.err
				.println("Usage: obclient --host hostname --port port number");
		System.err.println("");
	}

	public void processArgs(String[] args) {
		CommandLineParser parser = new PosixParser();
		try {
			cmd = parser.parse(options, args);
		} catch (Exception e) {
			printUsage();
			e.printStackTrace();
			System.exit(1);
		}

		if (cmd.hasOption(HOST))
			host = cmd.getOptionValue(HOST);
		if (cmd.hasOption(PORT))
			port = Integer.parseInt(cmd.getOptionValue(PORT));

		if (host == null || port == 0) {
			System.err.println("invalid argument.");
			printUsage();
			System.exit(2);
		}
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}
}
