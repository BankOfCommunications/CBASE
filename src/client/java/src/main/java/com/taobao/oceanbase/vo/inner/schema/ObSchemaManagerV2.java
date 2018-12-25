package com.taobao.oceanbase.vo.inner.schema;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.taobao.oceanbase.network.codec.Serialization;
import com.taobao.oceanbase.util.Const;

public class ObSchemaManagerV2 {

	class ObColumnNameKey {
		String tableName;
		String columnName;
	}

	class ObColumnIdKey {
		long tableId;
		long columnId;
	}

	class ObColumnInfo {
		ObColumnSchemaV2 head;
		int tableBeginIndex;
	}

	class ObColumnGroupHelper {
		long tableId;
		long columnGroupId;
	}

	private int schemaMagic;
	private int version;
	private long timestamp;
	private long maxTableId;
	private long columnNums;
	private long tableNums;

	private String appName;
	private List<ObTableSchema> tableInfos = new ArrayList<ObTableSchema>();
	private List<ObColumnSchemaV2> columns = new ArrayList<ObColumnSchemaV2>();

	// private boolean dropColumnGroup;
	// private boolean hashSorted;
	// private Map<ObColumnNameKey, ObColumnInfo> columnNameMap;
	// private Map<ObColumnIdKey, ObColumnInfo> columnIdMap;

	private long columnGroupNums;
	private List<ObColumnGroupHelper> columnGroups = new ArrayList<ObSchemaManagerV2.ObColumnGroupHelper>();

	public void deserialize(ByteBuffer buffer) {
		schemaMagic = Serialization.decodeInt(buffer);
		if (schemaMagic != ObSchemaCons.OB_SCHEMA_MAGIC_NUMBER) {
			throw new IllegalArgumentException("schema magic invalid, expect: "
					+ ObSchemaCons.OB_SCHEMA_MAGIC_NUMBER + ", but is: "
					+ schemaMagic);
		}
		version = Serialization.decodeVarInt(buffer);
		timestamp = Serialization.decodeVarLong(buffer);
		maxTableId = Serialization.decodeVarLong(buffer);
		columnNums = Serialization.decodeVarLong(buffer);
		tableNums = Serialization.decodeVarLong(buffer);
		appName = Serialization.decodeString(buffer, Const.NO_CHARSET);
		if (tableNums < 0 || tableNums > ObSchemaCons.OB_MAX_TABLE_NUMBER) {
			throw new IllegalArgumentException("table number invalid: "
					+ tableNums);
		}
		ObTableSchema schema = null;
		for (long i = 0; i < tableNums; ++i) {
			schema = new ObTableSchema();
			schema.deserialize(buffer);
			tableInfos.add(schema);
		}
		ObColumnSchemaV2 column = null;
		for (long i = 0; i < columnNums; ++i) {
			column = new ObColumnSchemaV2();
			column.deserialize(buffer);
			columns.add(column);
		}
	}

	public int getSchemaMagic() {
		return schemaMagic;
	}

	public int getVersion() {
		return version;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public long getMaxTableId() {
		return maxTableId;
	}

	public long getColumnNums() {
		return columnNums;
	}

	public long getTableNums() {
		return tableNums;
	}

	public String getAppName() {
		return appName;
	}

	public List<ObTableSchema> getTableInfos() {
		return tableInfos;
	}

	public List<ObColumnSchemaV2> getColumns() {
		return columns;
	}

	public long getColumnGroupNums() {
		return columnGroupNums;
	}

	public List<ObColumnGroupHelper> getColumnGroups() {
		return columnGroups;
	}

}
