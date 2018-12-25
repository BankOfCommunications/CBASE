package com.taobao.oceanbase.parsermysql;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;

import com.taobao.oceanbase.ClientImpl;
import com.taobao.oceanbase.vo.InsertMutator;
import com.taobao.oceanbase.vo.Value;

public class ParseItem {
	private String itemInputFilePath;
	private static String clientIp = "";
	private static int port = 0;
	static String table = "collect_item";
	private static ClientImpl client;// a new client

	public ParseItem(String clientIpAndPort,String filePath) {
		itemInputFilePath = filePath;
		String[] ipAndPort = clientIpAndPort.split(":");
		clientIp = ipAndPort[0];
		port = (int) Long.parseLong(ipAndPort[1]);
		client = new ClientImpl();
		client.setIp(clientIp);
		client.setPort(port);
		client.init();
	}

	public void Parse() {
		BufferedReader itemBuffRead;
		int i = 0;
		try {
			itemBuffRead = new BufferedReader(new InputStreamReader(
					new FileInputStream(itemInputFilePath), "UTF-8"));
			String itemLine = null;

			// read a line
			while ((itemLine = itemBuffRead.readLine()) != null) {
				i++;
				String[] values = itemLine.split(new String(new byte[] { 01 },
						"UTF-8"));
				char itype = values[10].trim().charAt(0);
				long tid = Long.parseLong(values[0]);
				
				if (itype == '0'||itype=='1') {
				} else {
					throw new Exception("The " +i+" line " +" of itemFile "+itemInputFilePath +" itype is invalid,itype:" + itype);
				}
				CollectRowKey rowkey = new CollectRowKey(itype, tid);

				InsertMutator itemInsertMutator = new InsertMutator(table,
						rowkey);
				for (int count = 0; count < values.length; count++) {
					Value value = new Value();
					SimpleDateFormat df = new SimpleDateFormat(
							"yyyy-MM-dd HH:mm:ss");
					Long second;
					switch (count) {
					case 0:// item_id
						break;
					case 1:
						value = new Value();
						if (!values[count].isEmpty()) {
							value.setString(values[count]);
						}
						itemInsertMutator.addColumn("item_title", value);
						break;
					case 2:
						value = new Value();
						if (!values[count].isEmpty()) {
							value.setString(values[count]);
						}
						itemInsertMutator.addColumn("item_picurl", value);
						break;
					case 3:
						break;
					case 4:
						value = new Value();
						if (!values[count].isEmpty()) {
							value.setString(values[count]);
						}
						itemInsertMutator.addColumn("item_owner_nick", value);
						break;
					case 5:
						value = new Value();
						if (!values[count].isEmpty()) {
							value.setNumber(new Float(Float
									.parseFloat(values[count]) * 100)
									.longValue());
						}
						itemInsertMutator.addColumn("item_price", value);
						break;
					case 6:
						value = new Value();
						if (!values[count].isEmpty()) {
							second = df.parse(values[count]).getTime() / 1000;
							value.setSecond(second);
						}
						itemInsertMutator.addColumn("item_ends", value);
						break;
					case 7:
						value = new Value();
						if (!values[count].isEmpty()) {
							value.setString(values[count]);
						}
						itemInsertMutator.addColumn("item_proper_xml", value);
						break;
					case 8:
						break;
					case 9:
						break;
					case 10:// item_type
						break;
					case 11:
						break;
					case 12:
						break;
					case 13:
						value = new Value();
						if (!values[count].isEmpty()) {
							value.setNumber(Long.parseLong(values[count]));
						}
						itemInsertMutator.addColumn("item_collector_count",
								value);
						break;
					case 14:
						break;
					case 15:
						break;
					case 16:
						value = new Value();
						if (!values[count].isEmpty()) {
							value.setNumber(Long.parseLong(values[count]));
						}
						itemInsertMutator.addColumn("item_status", value);
						break;
					case 17:
						break;
					case 18:
						break;
					case 19:
						break;
					case 20:
						break;
					case 21:
						break;
					case 22:
						value = new Value();
						if (!values[count].isEmpty()) {
							value.setNumber(Long.parseLong(values[count]));
						}
						itemInsertMutator.addColumn("item_category", value);
						break;
					case 23:
						value = new Value();
						if (!values[count].isEmpty()) {
							value.setNumber(Long.parseLong(values[count]));
						}
						itemInsertMutator
								.addColumn("item_collect_count", value);
						break;
					case 27:
						break;
					case 29:
						value = new Value();
						if (!values[count].isEmpty()) {
							value.setNumber(Long.parseLong(values[count]));
						}
						itemInsertMutator.addColumn("item_owner_id", value);
						value = new Value();
						value.setNumber(Long.parseLong("1"));
						itemInsertMutator.addColumn("item_invalid", value);
						break;
					default:
						break;
					}
				}

				if (client.insert(itemInsertMutator).getResult() == false) {
					System.out.println("false:" + i);
					throw new Exception("i line is wrong");
				}
			}
			itemBuffRead.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		if (args.length >=2) {
			for (int i = 1; i < args.length;) {
				ParseItem parseItem = new ParseItem(args[0], args[i]);
				parseItem.Parse();
				i = i + 1;
			}
			System.out.println("parse over");
		} else {
			System.out.println("please input ipAndport,item and so on");
		}
	}
}
