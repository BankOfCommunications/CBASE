package com.taobao.mrsstable;

import java.text.SimpleDateFormat;
import java.util.Date;

public class MRLogger {
  private SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SS");
  private String name = null;

  private MRLogger(String name) {
    this.name = name;
  }

  @SuppressWarnings("rawtypes")
  static public MRLogger getLogger(Class clazz) {
    return new MRLogger(clazz.getName());
  }

  public void info(String s) {
    Date date = new Date();
    System.out.printf("[%s] %s %s %s\n", df.format(date), "INFO", name, s);
  }

  public void warn(String s) {
    Date date = new Date();
    System.err.printf("[%s] %s %s %s\n", df.format(date), "WARN", name, s);
  }

  public void error(String s) {
    Date date = new Date();
    System.err.printf("[%s] %s %s %s\n", df.format(date), "ERROR", name, s);
  }
}
