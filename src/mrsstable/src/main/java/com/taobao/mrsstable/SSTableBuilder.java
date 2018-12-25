package com.taobao.mrsstable;

import java.nio.ByteBuffer;

public class SSTableBuilder {
  public native int init(String schema, String syntax, String tableId, boolean isSkipInvalidRow);

  public native ByteBuffer append(ByteBuffer input, boolean is_first, boolean is_last, boolean is_include_min,
      boolean is_include_max);

  public native void close();
}