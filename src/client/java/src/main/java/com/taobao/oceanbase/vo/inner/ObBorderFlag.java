package com.taobao.oceanbase.vo.inner;

public final class ObBorderFlag {

	private static final byte INCLUSIVE_START = 0x01;
	private static final byte INCLUSIVE_END = 0x02;
	private static final byte MIN_VALUE = 0x04;
	private static final byte MAX_VALUE = 0x08;

	private byte data;

	public ObBorderFlag() {
	}

	public ObBorderFlag(boolean start, boolean end, boolean min, boolean max) {
		setData(start, INCLUSIVE_START);
		setData(end, INCLUSIVE_END);
		setData(min, MIN_VALUE);
		setData(max, MAX_VALUE);
	}

	private void setData(boolean value, byte flag) {
		data = value ? (data |= flag) : (data &= (~flag));
	}

	protected void setData(byte data) {
		this.data = data;
	}

	public byte getData() {
		return data;
	}
}