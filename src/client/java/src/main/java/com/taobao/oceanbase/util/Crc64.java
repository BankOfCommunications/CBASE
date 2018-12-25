package com.taobao.oceanbase.util;

public class Crc64 {
	
	private class Integer
	{
		public Integer(int i)
		{
			value_ = i;
		}
		public int intValue()
		{
			return value_;
		}
		public void inc(int i)
		{
			value_ += i;
		}
		private int value_;
	};
	
	private static final int CRC64_TABLE_SIZE = 256;
	private static long[] s_crc64_table = new long[CRC64_TABLE_SIZE];
	public static final long OB_DEFAULT_CRC64_POLYNOM = 0xD800000000000000L;
	
	private static final Crc64 instance = new Crc64();
	public static Crc64 getInstance() {
		return instance;
	}
	
	static {
		for (int i = 0; i < CRC64_TABLE_SIZE; i++)
	    {
			long shift = ((long)(i) & 0x00000000000000ff);
	        for (int j = 0; j < 8; j++)
	        {
	        	final long unsigned_mask = 0x7fffffffffffffffL;
	        	if (0 != (shift & 0x1L))
	        	{
	        		shift = ((shift >> 1) & unsigned_mask) ^ OB_DEFAULT_CRC64_POLYNOM;
	        	}
	        	else
	        	{
	        		shift >>= 1;
	        		shift &= unsigned_mask;
	        	}
	        }
	        s_crc64_table[i] = shift;
	        //System.out.println(s_crc64_table[i]);
	    }
	}
	
	/*
	  DO_1_STEP(uCRC64, pu8) uCRC64 = s_crc64_table[(uCRC64 ^ *pu8++) & 0xff] ^ (uCRC64 >> 8);
	*/
	private long do_1_step(final long uCRC64, final byte[] buffer, Integer pos)
	{
		long item = buffer[pos.intValue()] & 0x00000000000000ffL;
		final long unsigned_mask = 0x00ffffffffffffffL;
		final long byte_mask = 0x00000000000000ffL;
		long ret = (s_crc64_table[(int)((uCRC64 ^ item) & byte_mask)] ^ ((uCRC64 >> 8) & unsigned_mask));
		pos.inc(1);
		//System.out.println("==="+item+"==="+((uCRC64 ^ item) & byte_mask)+"==="+((uCRC64 >> 8) & unsigned_mask)+"==="+uCRC64+"==="+ret);
		return ret;
	}
	
	/*
		#define DO_2_STEP(uCRC64, pu8)  DO_1_STEP(uCRC64, pu8); DO_1_STEP(uCRC64, pu8);
	*/
	private long do_2_step(final long uCRC64, final byte[] buffer, Integer pos)
	{
		long tmp = do_1_step(uCRC64, buffer, pos);
		return do_1_step(tmp, buffer, pos);
	}
	
    /*
		#define DO_4_STEP(uCRC64, pu8)  DO_2_STEP(uCRC64, pu8); DO_2_STEP(uCRC64, pu8);
	*/
	private long do_4_step(final long uCRC64, final byte[] buffer, Integer pos)
	{
		long tmp = do_2_step(uCRC64, buffer, pos);
		return do_2_step(tmp, buffer, pos);
	}
	
	/*
    	#define DO_8_STEP(uCRC64, pu8)  DO_4_STEP(uCRC64, pu8); DO_4_STEP(uCRC64, pu8);
    */
	private long do_8_step(final long uCRC64, final byte[] buffer, Integer pos)
	{
		long tmp = do_4_step(uCRC64, buffer, pos);
		return do_4_step(tmp, buffer, pos);
	}
	
	public long crc64(final byte[] buffer)
	{
		return crc64(0, buffer);
	}
	
	public long crc64(final long uCRC64, final byte[] buffer)
	{	
		long ret = uCRC64;
		int cb = buffer.length;
		Integer pos = new Integer(0);
		
        if (cb != 0)
        {
            while (cb >= 8)
            {
                ret = do_8_step(ret, buffer, pos);
                cb -= 8;
                //System.out.println(ret);
                //System.out.println(cb);
                //System.out.println(pos.v);
                //System.out.println("==========");
            }
            //System.out.println("=========="+cb+"==========");
    
            while (cb > 0) 
            {
                ret = do_1_step(ret, buffer, pos);
            	cb--;
            	//System.out.println(ret);
                //System.out.println(cb);
                //System.out.println(pos.v);
                //System.out.println("==========");
            }
        }
        return ret;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String str = "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffff1234567890aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffff1234567890aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffff1234567890aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffff1234567890";
		System.out.println(str.length());
		System.out.println(Crc64.getInstance().crc64(0, str.getBytes()));
	}

}

