package com.taobao.oceanbase.vo.inner;

public class ObReadParam {

	private long isCached = 1;
	private long isReadMaster = 1;
	private long beginVersion;
	private long endVersion;

	public void setCached(boolean isCached) {
		this.isCached = isCached ? 1 : 0;
	}

	protected long isCached() {
		return isCached;
	}
	
	public void setReadConsistency(boolean isReadConsistency) {
		this.isReadMaster = isReadConsistency ? 1 : 0;
	}
	
	public long isReadConsistency() {
		return isReadMaster;
	}

	public long getBeginVersion() {
		return beginVersion;
	}

	public void setBeginVersion(long beginVersion) {
		this.beginVersion = beginVersion;
	}

	public long getEndVersion() {
		return endVersion;
	}

	public void setEndVersion(long endVersion) {
		this.endVersion = endVersion;
	}

}