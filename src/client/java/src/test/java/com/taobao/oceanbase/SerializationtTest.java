package com.taobao.oceanbase;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import junit.framework.Assert;
import junit.framework.TestCase;

import com.taobao.oceanbase.vo.inner.ObObj;

public class SerializationtTest extends TestCase {

	InputStream file = null;

	public void testByte() throws Exception {
		file = getFile("D:\\gfile\\object");
		byte[] content = new byte[file.available()];
		file.read(content);
		ByteBuffer buffer = ByteBuffer.wrap(content);
		ObObj obj = new ObObj();
		obj.deserialize(buffer);
		Assert.assertEquals(9871345, (long) (Long) obj.getValue());
		obj.deserialize(buffer);
		Assert.assertEquals("Hello", obj.getValue());
		obj.deserialize(buffer);
		Assert.assertEquals(null, obj.getValue());
		obj.deserialize(buffer);
		Assert.assertEquals(59.16f, obj.getValue());
		obj.deserialize(buffer);
		Assert.assertEquals(59.167d, obj.getValue());
		obj.deserialize(buffer);
		Assert.assertEquals(1234898766, (long) (Long) obj.getValue());
		obj.deserialize(buffer);
		Assert.assertEquals(2348756909000000L, (long) (Long) obj.getValue());
	}

	protected void tearDown() throws Exception {
		file.close();
	}

	private FileInputStream getFile(String filePath)
			throws FileNotFoundException {
		return new FileInputStream(filePath);
	}
}