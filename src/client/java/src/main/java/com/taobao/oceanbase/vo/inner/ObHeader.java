package com.taobao.oceanbase.vo.inner;

import java.nio.ByteBuffer;

import com.taobao.oceanbase.network.codec.Serialization;
import com.taobao.oceanbase.util.Crc64;

public class ObHeader {

	public static final short DEFAULT_MAGIC = (short) 0xBCDE;
	public static final int DEFAULT_LENGTH = 32;
	public static final short DEFAULT_VERSION = 0;
	
	private short magic = 0;
	private short headerLength = 0;
	private short version = 0;
	private short headerChecksum = 0;
	private int dataOriginLength = 0;
	private int dataCompressLength = 0;
	private long dataChecksum = 0;
	private long reserved = 0;

	public void setRecord(final ByteBuffer buffer, final int length) {
		magic = DEFAULT_MAGIC;
		headerLength = DEFAULT_LENGTH;
		version = DEFAULT_VERSION;
		
		dataOriginLength = length;
		dataCompressLength = length;
		dataChecksum = Crc64.getInstance().crc64(buffer.array());
		
		setHeaderChecksum();		
	}
	public void setHeaderChecksum() {
		headerChecksum = 0;
		short checkSum = 0;
		
		checkSum = formatLong(magic, checkSum);
		checkSum = (short) (checkSum ^ headerLength);
		checkSum = (short) (checkSum ^ version);
		checkSum = (short) (checkSum ^ headerChecksum);
		checkSum = (short) (checkSum ^ reserved);
		checkSum = formatInt(dataOriginLength, checkSum);
		checkSum = formatInt(dataCompressLength, checkSum);
		checkSum = formatLong(dataChecksum, checkSum);
		
		headerChecksum = checkSum;
	}
	
	public boolean checkHeader() {
		boolean ret = true;		
		short checkSum = 0;
		
		checkSum = formatLong(magic, checkSum);
		checkSum = (short) (checkSum ^ headerLength);
		checkSum = (short) (checkSum ^ version);
		checkSum = (short) (checkSum ^ headerChecksum);
		checkSum = (short) (checkSum ^ reserved);
		checkSum = formatInt(dataOriginLength, checkSum);
		checkSum = formatInt(dataCompressLength, checkSum);
		checkSum = formatLong(dataChecksum, checkSum);
		
		if (checkSum != 0) {
			ret = false;
		}
		
		return ret;
	}
	
	public boolean checkMagic() {
		return DEFAULT_MAGIC == this.magic;
	}
	
	public boolean checkCheckSum(final byte[] buffer) {
		long checksum = Crc64.getInstance().crc64(buffer);
		
		return checksum == this.dataChecksum;
	}
	
	public boolean checkRecord(final byte[] buffer) {
		boolean ret = true;
		
		ret = checkMagic();
		
		if (ret) {
			ret = checkHeader();
		}
		
		if (ret) {
			ret = checkCheckSum(buffer);
		}
		
		return ret;
	}
	
	private short formatLong(final long value, short checksum) {
		int i = 0;
		short checkSum = checksum;
		while (i < 4) {
			checkSum = (short) (checkSum ^ ((value >> i * 16) & 0xFFFF));
			++i;
		}
		return checkSum;
	}
	private short formatInt(final int value, short checksum) {
		int i = 0;
		short checkSum = checksum;
		while (i < 2) {
			checkSum = (short) (checkSum ^ ((value >> i * 16) & 0xFFFF));
			++i;
		}
		return checkSum;
	}
	public void serialize(ByteBuffer buffer) {
		buffer.put(Serialization.encode(magic));
		buffer.put(Serialization.encode(headerLength));
		buffer.put(Serialization.encode(version));
		buffer.put(Serialization.encode(headerChecksum));
		buffer.put(Serialization.encode(reserved));
		buffer.put(Serialization.encode(dataOriginLength));
		buffer.put(Serialization.encode(dataCompressLength));
		buffer.put(Serialization.encode(dataChecksum));
	}

	public int getSize() {
		return DEFAULT_LENGTH;
	}

	public ObHeader deserialize(ByteBuffer buffer) {
		byte short_bytes[] = new byte[2];
		buffer.get(short_bytes);
		magic = Serialization.decodeShort(short_bytes);
		buffer.get(short_bytes);
		headerLength = Serialization.decodeShort(short_bytes);
		buffer.get(short_bytes);
		version = Serialization.decodeShort(short_bytes);
		buffer.get(short_bytes);		
		headerChecksum = Serialization.decodeShort(short_bytes);
		reserved = Serialization.decodeLong(buffer);
		dataOriginLength = Serialization.decodeInt(buffer);
		dataCompressLength = Serialization.decodeInt(buffer);
		dataChecksum = Serialization.decodeLong(buffer);
		
		return this;
	}

	public String toString() {				
		StringBuffer sb = new StringBuffer();
		sb.append("[magic=").append(magic).append(",");
		sb.append("headerLength=").append(headerLength).append(",");
		sb.append("version=").append(version).append(",");
		sb.append("headerChecksum=").append(headerChecksum).append(",");
		sb.append("dataOriginLength=").append(dataOriginLength).append(",");
		sb.append("dataCompressLength=").append(dataCompressLength).append(",");
		sb.append("dataChecksum=").append(dataChecksum).append(",");
		sb.append("reserved=").append(reserved).append("]");
		
		return sb.toString();
	}
}