package com.taobao.oceanbase.parsermysql;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;

import com.taobao.oceanbase.ClientImpl;
import com.taobao.oceanbase.vo.InsertMutator;
import com.taobao.oceanbase.vo.Value;

public class ParseInfo {
	private String infoInputFilePath;
	private String itemInputFilePath;
	private static String clientIp = "";
	private static int port = 0;
	static String table = "collect_info";
	private static ClientImpl client;// a new client

	public ParseInfo(String clientIpAndPort, String filePath,
			String itemFilePath) {
		infoInputFilePath = filePath;
		itemInputFilePath = itemFilePath;

		String[] ipAndPort = clientIpAndPort.split(":");
		clientIp = ipAndPort[0];
		port = (int) Long.parseLong(ipAndPort[1]);
		client = new ClientImpl();
		client.setIp(clientIp);
		client.setPort(port);
		client.init();
	}

	public void Parse() {
		BufferedReader infoBuffRead;
		BufferedReader itemBuffRead;
		int i = 0;
		try {
			infoBuffRead = new BufferedReader(new InputStreamReader(
					new FileInputStream(infoInputFilePath), "UTF-8"));
			itemBuffRead = new BufferedReader(new InputStreamReader(
					new FileInputStream(itemInputFilePath), "UTF-8"));
			String infoLine = null;
			String itemLine = null;
			// read a line
			while ((infoLine = infoBuffRead.readLine()) != null) {
				String[] infoValues = infoLine.split(new String(
						new byte[] { 01 }, "UTF-8"));
				long uid = Long.parseLong(infoValues[1]);
				char itype = 0;
				long tid = 0;
				String[] itemValues;
				if ((itemLine = itemBuffRead.readLine()) != null) {
					itemValues = itemLine.split(new String(
							new byte[] { 01 }, "UTF-8"));
					i++;
				}else{
					itemBuffRead = new BufferedReader(new InputStreamReader(
							new FileInputStream(itemInputFilePath), "UTF-8"));
					itemValues = itemBuffRead.readLine().split(new String(
							new byte[] { 01 }, "UTF-8"));
					i=1;
				}
				// item_id
				tid = Long.parseLong(itemValues[0]);
				// item_type
				
				itype = itemValues[10].trim().charAt(0);
				if (itype == '0'||itype=='1') {
				} else {
					throw new Exception("The " +i+" line " +" of itemFile "+itemInputFilePath+" itype is invalid,itype:" + itype);
				}
				CollectRowKey rowkey = new CollectRowKey(uid, itype, tid);
				InsertMutator infoInsertMutator = new InsertMutator(table,
						rowkey);
				for (int count = 0; count < infoValues.length; count++) {
					Value value = new Value();
					SimpleDateFormat df = new SimpleDateFormat(
							"yyyy-MM-dd HH:mm:ss");
					Long second;
					switch (count) {
					case 0:
						break;
					case 1:// user_id
						break;
					case 2:
						value = new Value();
						if (!infoValues[count].isEmpty()) {
							value.setNumber(Long.parseLong(infoValues[count]));
						}
						infoInsertMutator.addColumn("info_is_shared", value);
						break;
					case 3:
						break;
					case 4:
						break;
					case 5:
						value = new Value();
						if (!infoValues[count].isEmpty()) {
							value.setNumber(Long.parseLong(infoValues[count]));
						}
						infoInsertMutator.addColumn("info_status", value);
						break;
					case 6:
						break;
					case 7:
						break;
					case 8:
						value = new Value();
						if (!infoValues[count].isEmpty()) {
							second = df.parse(infoValues[count]).getTime() / 1000;
							value.setSecond(second);
						}
						infoInsertMutator.addColumn("info_collect_time", value);
						break;
					case 9:
						value = new Value();
						if (!infoValues[count].isEmpty()) {
							value.setString(infoValues[count]);
						}
						infoInsertMutator.addColumn("info_user_nick", value);
						break;
					case 10:
						value = new Value();
						if (!infoValues[count].isEmpty()) {
							value.setString(infoValues[count]);
						}
						infoInsertMutator.addColumn("info_tag", value);
						break;
					case 11:
						break;
					case 12:
						break;
					default:
						break;

					}
				}
				if (client.insert(infoInsertMutator).getResult() == false) {
					System.out.println("false:" + i);
					throw new Exception("i line is wrong");
				}
			}
			infoBuffRead.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		if (args.length >= 3 && args.length % 2 == 1) {
			for (int i = 1; i < args.length;) {
				ParseInfo parseInfo = new ParseInfo(args[0], args[i],
						args[i + 1]);
				parseInfo.Parse();
				i = i + 2;
			}
			System.out.println("parse over");
		} else {
			System.out.println("please input ipAndport,info,item and so on");
		}
		
	}
}
