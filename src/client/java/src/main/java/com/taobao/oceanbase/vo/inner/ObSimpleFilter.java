package com.taobao.oceanbase.vo.inner;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ObSimpleFilter {

	List<ObSimpleCond> condList = new ArrayList<ObSimpleCond>();
	
	public void addCondition(ObSimpleCond cond) {
		condList.add(cond);
	}
	
	public void serialize(ByteBuffer buffer) {
		if (condList != null && condList.size() > 0) {
			for (ObSimpleCond cond : condList) {
				cond.serialize(buffer);
			}
		}
	}
	
	public int getSize() {
		int size = 0;
		if (condList != null && condList.size() > 0) {
			for (ObSimpleCond cond : condList) {
				size += cond.getSize();
			}
		}
		return size;
	}
	
	public boolean isValid() {
		boolean res = true;
		
		for (ObSimpleCond cond : condList) {
			if (!cond.isValid()) {
				res = false;
			}
		}
		return res;
	}
}
 