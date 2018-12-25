package com.taobao.mrsstable.test;

import java.nio.ByteBuffer;

import junit.framework.Assert;
import junit.framework.TestCase;


public class ByteBufferTest extends TestCase {

	private static final int BUF_SIZE = 1024 * 32;
	
	public void testByteBuffer() {
		ByteBuffer buffer = ByteBuffer.allocateDirect(BUF_SIZE);
		Assert.assertEquals(0, buffer.position());
		Assert.assertEquals(BUF_SIZE, buffer.capacity());
		Assert.assertEquals(BUF_SIZE, buffer.limit());
		Assert.assertEquals(BUF_SIZE, buffer.remaining());
		Assert.assertTrue(buffer.isDirect());
		Assert.assertFalse(buffer.hasArray());
		
		buffer.clear();
		Assert.assertEquals(0, buffer.position());
		Assert.assertEquals(BUF_SIZE, buffer.capacity());
		Assert.assertEquals(BUF_SIZE, buffer.limit());
		Assert.assertEquals(BUF_SIZE, buffer.remaining());
		Assert.assertTrue(buffer.isDirect());
		Assert.assertFalse(buffer.hasArray());			
		
		byte[] content = {0, 1, 2, 3, 4};
		buffer.put(content);
		Assert.assertEquals(5, buffer.position());
		Assert.assertEquals(BUF_SIZE, buffer.capacity());
		Assert.assertEquals(BUF_SIZE, buffer.limit());	
		Assert.assertEquals(BUF_SIZE - 5, buffer.remaining());
		
		buffer.put(content);
		Assert.assertEquals(10, buffer.position());
		Assert.assertEquals(BUF_SIZE, buffer.capacity());
		Assert.assertEquals(BUF_SIZE, buffer.limit());
		Assert.assertEquals(BUF_SIZE - 10, buffer.remaining());
		
		buffer.flip();
		Assert.assertEquals(0, buffer.position());
		Assert.assertEquals(BUF_SIZE, buffer.capacity());
		Assert.assertEquals(10, buffer.limit());
		Assert.assertEquals(10, buffer.remaining());
		
		ByteBuffer newBuffer = buffer.slice();
		Assert.assertEquals(0, newBuffer.position());
		Assert.assertEquals(10, newBuffer.capacity());
		Assert.assertEquals(10, newBuffer.limit());
		Assert.assertEquals(10, newBuffer.remaining());
		Assert.assertTrue(newBuffer.isDirect());
		
		buffer.clear();
		Assert.assertEquals(0, buffer.position());
		Assert.assertEquals(BUF_SIZE, buffer.capacity());
		Assert.assertEquals(BUF_SIZE, buffer.limit());
		Assert.assertEquals(BUF_SIZE, buffer.remaining());
		Assert.assertTrue(buffer.isDirect());
		Assert.assertFalse(buffer.hasArray());	
		
		Assert.assertEquals(0, newBuffer.position());
		Assert.assertEquals(10, newBuffer.capacity());
		Assert.assertEquals(10, newBuffer.limit());
		Assert.assertEquals(10, newBuffer.remaining());
		Assert.assertTrue(newBuffer.isDirect());		
	}
}
