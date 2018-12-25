package com.taobao.oceanbase.cli;

import java.io.File;
import java.util.List;

import jline.ConsoleReader;
import jline.History;

import com.taobao.oceanbase.OBQLClient;
import com.taobao.oceanbase.OBQLClientImpl;
import com.taobao.oceanbase.vo.Result;
import com.taobao.oceanbase.vo.RowData;

public class OBClient {

	private static OBQLClient client_instance = null;
	private static final String HISFILENAME = ".obclient_history";

	public static void main(String[] args) throws Exception {
		ObOptions opt = new ObOptions();
		opt.processArgs(args);

		client_instance = new OBQLClientImpl();
		((OBQLClientImpl) client_instance).setIp(opt.getHost());
		((OBQLClientImpl) client_instance).setPort(opt.getPort());

		try {
			((OBQLClientImpl) client_instance).init();
		} catch (Exception e) {
			System.err.println("connect to " + opt.getHost() + ":"
					+ opt.getPort() + " failed");
			e.printStackTrace();
			System.exit(3);
		}

		ConsoleReader reader = new ConsoleReader();
		reader.setBellEnabled(false);

		String hisfile = System.getProperty("user.home") + File.separator
				+ HISFILENAME;

		try {
			History his = new History(new File(hisfile));
			reader.setHistory(his);
		} catch (Exception e) {
			System.err.println("unable to open history file, history disabled");
		}

		System.out.println("oceanbase client 1.0 inited");
		System.out.println("connected to rootserver: " + opt.getHost() + ":"
				+ opt.getPort());

		String line;
		// line = "select user_nick FROM collect_info WHERE ROWKEY = 12345";
		// processCmd(line);

		while ((line = reader.readLine("oceanbase=> ")) != null) {
			try {
				processCmd(line.trim());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private static void processCmd(String line) throws Exception {
		if (line.equalsIgnoreCase("exit")) {
			System.out.println("bye.");
			System.exit(0);
		}

		if (line.startsWith("select") || line.startsWith("SELECT")) {
			Result<List<RowData>> result = client_instance.query(line);
			System.out.println(result);
		}
		if (line.startsWith("insert") || line.startsWith("INSERT")) {
			Result<Boolean> result = client_instance.insert(line);
			System.out.println(result);
		}
		if (line.startsWith("update") || line.startsWith("UPDATE")) {
			Result<Boolean> result = client_instance.update(line);
			System.out.println(result);
		}
		if (line.startsWith("delete") || line.startsWith("DELETE")) {
			Result<Boolean> result = client_instance.delete(line);
			System.out.println(result);
		}
	}
}