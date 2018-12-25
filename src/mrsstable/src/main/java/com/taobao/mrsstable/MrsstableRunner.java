package com.taobao.mrsstable;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

/**
 * A utility to help run MRGenSstable class. It is as the same as ToolRunner,
 * exclude the MrsstableOptionsParser is used instead of GenericOptionsParser
 *
 * @see Tool
 * @see ToolRunner
 * @see GenericOptionsParser
 */
public class MrsstableRunner {
  private static final MRLogger LOG = MRLogger.getLogger(MrsstableRunner.class);

  /**
   * add mrsstable additional options
   *
   * @return
   */
  @SuppressWarnings("static-access")
  private static Options buildMrsstableOptions() {
    Options options = new Options();
    Option tableName = OptionBuilder.withArgName("table name").hasArg()
        .withDescription("specify the table name of the table to run mrsstable tool.").create("table_name");
    Option tableId = OptionBuilder.withArgName("table id").hasArg()
        .withDescription("specify the id of the table to run mrsstable tool.").create("table_id");
    Option rowkeyDesc = OptionBuilder
        .withArgName("rowkey desc")
        .hasArg()
        .withDescription(
            "rowkey description, it defines the rowkey formation"
                + " format = \"clomn index in rowkey\",\"column index in origin line\",\"the column type\"")
        .create("rowkey_desc");
    Option sampler = OptionBuilder.withArgName("sampler name").hasArg()
        .withDescription("specify sampler file: specify, random, interval, split.").create("sampler");
    Option delim = OptionBuilder.withArgName("delim").hasArg()
        .withDescription("delimeter in hdfs data file").create("delim");
    Option null_flag = OptionBuilder.withArgName("null_flag").hasArg()
        .withDescription("null_flag in hdfs data file").create("null_flag");
    Option job_name = OptionBuilder.withArgName("job_name").hasArg()
        .withDescription("job name for mapreduce").create("job_name");
    Option partition_file = OptionBuilder.withArgName("partition_file").hasArg()
        .withDescription("partition file path in hdfs").create("partition_file");
    options.addOption(tableName);
    options.addOption(tableId);
    options.addOption(rowkeyDesc);
    options.addOption(sampler);
    options.addOption(delim);
    options.addOption(null_flag);
    options.addOption(job_name);
    options.addOption(partition_file);
    return options;
  }

  /**
   * overload the param of configuration file(specified with -conf)
   *
   * @param conf
   * @param line
   * @throws IOException
   */
  private static void processMrsstableOptions(Configuration conf, CommandLine line) throws IOException {
    if (line.hasOption("table_name")) {
      String value = line.getOptionValue("table_name");
      conf.set("mrsstable.table.name", value);
      LOG.info("overload mrsstable.table.name with cmd opertion table_name: " + value);
    }
    
    if (line.hasOption("table_id")) {
      String value = line.getOptionValue("table_id");
      conf.set("mrsstable.table.id", value);
      LOG.info("overload mrsstable.table.id with cmd opertion table_id: " + value);
    }

    if (line.hasOption("rowkey_desc")) {
      String value = line.getOptionValue("rowkey_desc");
      conf.set("mrsstable.rowkey.desc", value);
      LOG.info("overload mrsstable.rowkey.desc with cmd opertion rowkey_desc: " + value);
    }

    if (line.hasOption("job_name")) {
      String value = line.getOptionValue("job_name");
      conf.set("mrsstable.jobname", value);
      LOG.info("set mrsstable.job_name: " + value);
    }

    if (line.hasOption("partition_file")) {
      String value = line.getOptionValue("partition_file");
      conf.set("mrsstable.partition.file", value);
      LOG.info("set mrsstable.partition.file: " + value);
    }

    if (line.hasOption("delim")) {
      String value = line.getOptionValue("delim");
      int n = Integer.parseInt(value);
      if (n < 0 || n >= 256){
        LOG.error("option delimiter must between 0~255");
        throw new IOException("option delimiter must between 0~255");
      } else {
        char c = (char) n;
        String delim = String.valueOf(c);
        String escapedDelim = StringEscapeUtils.escapeJava(delim);
        conf.set("mrsstable.primary.delimeter", escapedDelim);
        LOG.info("overload mrsstable.primary.delimeter with cmd option delimiter: "
            + value + "[" + delim + "] escaped delim: " + escapedDelim);
      }
    }

    if (line.hasOption("null_flag")) {
      String value = line.getOptionValue("null_flag");
      int n = Integer.parseInt(value);
      if (n < 0 || n >= 256){
        LOG.error("option null_flag must between 0~255");
        throw new IOException("option null_flag must between 0~255");
      } else {
        char c = (char) n;
        String null_flag = String.valueOf(c);
        String nullFlag = StringEscapeUtils.escapeJava(null_flag);
        conf.set("mrsstable.primary.nullflag", nullFlag);
        LOG.info("overload mrsstable.primary.nullflag with cmd option null_flag: "
            + value + "[" + null_flag + "] escaped null_flag: " + nullFlag);
      }
    }
  }

  public static int run(Configuration conf, Tool tool, String[] args) throws Exception {
    if (conf == null) {
      conf = new Configuration();
    }

    Options options = buildMrsstableOptions();

    GenericOptionsParser parser = new GenericOptionsParser(conf, options, args);
    CommandLine commandLine = parser.getCommandLine();
    processMrsstableOptions(conf, commandLine);

    tool.setConf(conf);

    String[] toolArgs = parser.getRemainingArgs();
    return tool.run(toolArgs);
  }

  public static int run(Tool tool, String[] args) throws Exception {
    return run(tool.getConf(), tool, args);
  }

  public static void printMrsstableCommandUsage(PrintStream out) {
    out.printf("Usage: %s [options] <input dirs> <output dir>\n", MrsstableRunner.class.getSimpleName());
    out.println("options supported are (overload the param of configuration file, which is specified with -conf) : ");
    out.println("-table_id <table id>           overload mrsstable.table.id");
    out.println("-rowkey_desc <rowkey desc>     overload mrsstable.rowkey.desc");
    out.println("-sampler <sampler type>        overload mrsstable.presort.sampler");
    out.println("-delim <delim>                 overload mrsstable.primary.delimeter");
    out.println("-null_flag <null_flag>         overload mrsstable.primary.nullflag");

    GenericOptionsParser.printGenericCommandUsage(out);
  }

  public static void main(String[] args) throws Exception {
    int res = MrsstableRunner.run(new Configuration(), new MRGenSstable(), args);
    System.exit(res);
  }
}
