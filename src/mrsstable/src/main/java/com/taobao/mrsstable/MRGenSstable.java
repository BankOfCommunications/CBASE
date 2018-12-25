package com.taobao.mrsstable;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidParameterException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Tool;

import com.taobao.mrsstable.MRGenSstable;
import com.taobao.mrsstable.TextInputSampler;

public class MRGenSstable extends Configured implements Tool {

  private static final MRLogger LOG = MRLogger.getLogger(MRGenSstable.class);
  private static String PRIMARY_DELIM;
  private static String NULL_FLAG;

  boolean writePartsFile = false;

  private static final String LIB_MRSSTABLE_NAME = "libmrsstable.so";
  private static final String LIB_NONE_OB_NAME = "libnone.so";
  private static final String LIB_LZO_ORG_NAME = "liblzo2.so";
  private static final String LIB_LZO_OB_NAME = "liblzo_1.0.so";
  private static final String LIB_SNAPPY_ORG_NAME = "libsnappy.so";
  private static final String LIB_SNAPPY_OB_NAME = "libsnappy_1.0.so";

  private static final String RANGE_FILE_NAME = "config/range.lst";
  private static final String PARTITIOIN_FILE_NAME = "_partition_file";
  private static final String TABLE_INFO_FILE = "_table_info";
  private static final String CONFIG_DIR = "config";

  public static String getEscapeStr(Configuration conf, String confSetting, String defaultSetting) {
    String delim = conf.get(confSetting);
    return delim == null ? defaultSetting : StringEscapeUtils.unescapeJava(delim);
  }

  public static class RowKeyDesc {
    // type must same with common/ob_obj_type.h
    public final int INT = 1;
    public final int PRECISE_DATETIME = 5;
    public final int VARCHAR = 6;
    private static final String INDEX_DELIM = "-";
    private int[] orgColumnIndex;
    private int[] columnType;
    private int[] size;
    private int length;
    private int maxColumnId;
    private SimpleDateFormat dfList[] = {
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"),// 0
        new SimpleDateFormat("yyyy-MM-dd"),         // 1
        new SimpleDateFormat("yyyyMMdd HH:mm:ss"),  // 2
        new SimpleDateFormat("yyyyMMddHHmmss"),     // 3
        new SimpleDateFormat("yyyyMMdd")            // 4
    };

    public String getSimpleDesc() {
      StringBuffer desc = new StringBuffer();
      for(int i=0; i<length; ++i){
        if (i!=0){
          desc.append(",");
        }
        if (columnType[i] == VARCHAR) {
          desc.append(String.format("%d-%d", columnType[i], size[i]));
        } else {
          desc.append(String.format("%d", columnType[i]));
        }
      }
      return desc.toString();
    }

    void checkRowKeyDesc(String rowkeyDescStr) {
      StringBuffer desc = new StringBuffer();
      for (int i = 0; i < length; ++i) {
        if (i != 0) {
          desc.append(",");
        }
        if (columnType[i] == VARCHAR) {
          desc.append(String.format("%d-%d-%d", orgColumnIndex[i], columnType[i], size[i]));
        } else {
          desc.append(String.format("%d-%d", orgColumnIndex[i], columnType[i]));
        }
      }
      String newRowkeyDescStr = desc.toString();
      if (!newRowkeyDescStr.equals(rowkeyDescStr)) {
        throw new InvalidParameterException("Parameter mrsstable.rowkey.desc[" + rowkeyDescStr
            + "] is not same as parsed rowkey_desc[" + newRowkeyDescStr + "]");
      }
    }

    public RowKeyDesc buildRowKeyDesc(JobConf conf) {
      String rowkeyDescStr = conf.get("mrsstable.rowkey.desc", "");
      try {
        if (rowkeyDescStr.isEmpty()) {
          throw new InvalidParameterException("not mrsstable.rowkey.desc config found");
        }

        String[] rowKeyDesc = rowkeyDescStr.split(",");
        if (rowKeyDesc.length <= 0) {
          throw new InvalidParameterException("wrong mrsstable.rowkey.desc config");
        }
        orgColumnIndex = new int[rowKeyDesc.length];
        columnType = new int[rowKeyDesc.length];
        size = new int[rowKeyDesc.length];
        length = rowKeyDesc.length;

        for (int i = 0; i < rowKeyDesc.length; i++) {
          String[] columnDesc = rowKeyDesc[i].split(INDEX_DELIM, -1);
          if (columnDesc.length == 2 || columnDesc.length == 3) {
            orgColumnIndex[i] = Integer.parseInt(columnDesc[0]);
            columnType[i] = Integer.parseInt(columnDesc[1]);
            if (columnDesc.length == 3){
              size[i] = Integer.parseInt(columnDesc[2]);
            } else {
              size[i] = 0;
            }
            // check size
            int expectSize = -1;
            switch(columnType[i]){
            case INT:
              expectSize = 0;
              break;
            case PRECISE_DATETIME:
              expectSize = 0;
              break;
            case VARCHAR:
              expectSize = size[i];
              break;
            default:
              throw new InvalidParameterException("miss col type:" + columnType[i]);
            }
            if (expectSize != size[i]){
              throw new InvalidParameterException("for rowkeyDesc ("+rowKeyDesc+")["+i+"] expect size["+expectSize+"] not equals to size["+size[i]+"]");
            }
          } else {
            throw new InvalidParameterException("each column desc of rowkey must include 3 part or 4 part");
          }
        }
      } catch (NumberFormatException e) {
        throw new InvalidParameterException("Parameter mrsstable.rowkey.desc is not set properly. "
            + "Use comma seperated number list");
      }
      assert (orgColumnIndex.length > 0);

      checkRowKeyDesc(rowkeyDescStr);
      this.maxColumnId = 0;
      for (int column : this.orgColumnIndex) {
        if (this.maxColumnId < column) {
          this.maxColumnId = column;
        }
      }

      return this;
    }

    public int getMaxColumnId() {
      return this.maxColumnId;
    }

    public int[] getOrgColumnIndex() {
      return orgColumnIndex;
    }

    public int[] getColumnType() {
      return columnType;
    }

    public SimpleDateFormat getDateFromat(String field){
      SimpleDateFormat df;

      if (field.indexOf("-") > -1) {
        if (field.indexOf(":") > -1) {
          df = dfList[0];//new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        } else {
          df = dfList[1];//new SimpleDateFormat("yyyy-MM-dd");
        }
      } else {
        if (field.indexOf(":") > -1) {
          df = dfList[2];//new SimpleDateFormat("yyyyMMdd HH:mm:ss");
        } else if (field.length() > 8) {
          df = dfList[3];//new SimpleDateFormat("yyyyMMddHHmmss");
        } else {
          df = dfList[4];//new SimpleDateFormat("yyyyMMdd");
        }
      }
      return df;
    }

    public boolean isRowkeyValid(String[] vec) {
      boolean is_valid = true;

      if (vec.length <= maxColumnId){
        is_valid = false;
      } else {
        for (int i = 0; i < orgColumnIndex.length; ++i) {
          if (!checkField(vec[orgColumnIndex[i]], i)){
            is_valid = false;
            break;
          }
        }
      }

      return is_valid;
    }

    public boolean checkField(String field, int idx)
    {
      boolean is_valid = true;
      if (idx >= length)
      { // should not reach here, if reach here, maybe come code logic is wrong
        is_valid = false;
        throw new InvalidParameterException("Parameter idx["+idx+"] is must less than length["+length+"]");
      }
      else
      {
        switch(columnType[idx]){
        case INT:
        {
          try {
            long value = Long.parseLong(field);
            if (value < (-9223372036854775807L-1) || value > 9223372036854775807L){
              is_valid = false;
            }
          } catch (NumberFormatException e) {
            System.out.println("failed to parse int [" + field + "]");
            is_valid = false;
          }
          break;
        }
        case VARCHAR:
        {
          if (field.length() > size[idx]){
            is_valid = false;
            System.out.println("varchar is too long(limit="+size[idx]+") [" + field + "]");
          }
          else if (field.length() == 1 && field.equals(NULL_FLAG))
          {
            is_valid = false;
            System.out.println("rowkey must not have null field [" + field + "]");
          }
          break;
        }
        case PRECISE_DATETIME:
        {
          SimpleDateFormat df = this.getDateFromat(field);

          try {
            df.parse(field);
          } catch (ParseException e) {
            System.out.println("failed to parse datetime [" + field + "]");
            is_valid = false;
          }
          break;
        }
        default:
          // should not reach here, if reach here, maybe come code logic is wrong
          is_valid = false;
          throw new InvalidParameterException("miss col type:" + columnType[idx]);
        }
      }
      return is_valid;
    }
  }

  public static class TokenizerMapper implements Mapper<Object, Text, Text, Text> {

    private static final MRLogger LOG = MRLogger.getLogger(TokenizerMapper.class);
    private static String PRIMARY_DELIM;
    private Text k = new Text();
    RowKeyDesc rowKeyDesc;
    private int[] keyColumnIndex;
    private int minValueColumnIdx = 0;
    boolean isSkipInvalidRow = false;

    public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      String[] vec = value.toString().split(PRIMARY_DELIM, -1);
      if (!rowKeyDesc.isRowkeyValid(vec)) {
        StringBuilder err_sb = new StringBuilder("invalid rowkey, skip this line, ");
        for (int i = 0; i < keyColumnIndex.length; i++) {
          err_sb.append("[index:" + i + ", type:" + rowKeyDesc.getColumnType()[i] + ", val:" + vec[keyColumnIndex[i]]
              + "], ");
        }
        if (isSkipInvalidRow) {
          LOG.warn(err_sb.toString());
          return;
        } else {
          throw new IOException(err_sb.toString());
        }
      }

      StringBuilder sb = new StringBuilder(vec[keyColumnIndex[0]]);
      for (int i = 1; i < keyColumnIndex.length; i++) {
        sb.append(PRIMARY_DELIM).append(vec[keyColumnIndex[i]]);
      }
      k.set(sb.toString());
      output.collect(k, value);
    }

    @Override
    public void configure(JobConf conf) {
      PRIMARY_DELIM = getEscapeStr(conf, "mrsstable.primary.delimeter", "\001");
      String skipInvalidRow = conf.get("mrsstable.skip.invalid.row", "0");
      if (skipInvalidRow.equalsIgnoreCase("1")) {
        isSkipInvalidRow = true;
      }
      rowKeyDesc = new RowKeyDesc();
      keyColumnIndex = rowKeyDesc.buildRowKeyDesc(conf).getOrgColumnIndex();
      if (keyColumnIndex.length <= 0) {
        throw new IllegalArgumentException("key columns count must greater than 0" + keyColumnIndex.length);
      }
      for (int i = 0; i < keyColumnIndex.length; i++) {
        if (keyColumnIndex[i] > minValueColumnIdx) {
          minValueColumnIdx = keyColumnIndex[i];
        }
      }
    }

    @Override
    public void close() throws IOException {

    }
  }

  public static class SSTableReducer implements Reducer<Text, Text, Text, Text> {

    @Override
    public void configure(JobConf conf) {
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void reduce(Text key, Iterator<Text> value, OutputCollector<Text, Text> output, Reporter arg3)
        throws IOException {
      while (value.hasNext()) {
        output.collect(key, value.next());
      }
    }
  }

  public static class TaggedKeyComparator extends Configured implements RawComparator<Text> {

    private static String PRIMARY_DELIM;
    private static String NULL_FLAG;
    RowKeyDesc rowKeyDesc;
    private boolean is_inited = false;

    public void init() {
      if (!is_inited) {
        if (PRIMARY_DELIM == null) {
          PRIMARY_DELIM = getEscapeStr(getConf(), "mrsstable.primary.delimeter", "\001");
        }
        if (NULL_FLAG == null) {
          NULL_FLAG = getEscapeStr(getConf(), "mrsstable.primary.nullflag", "\002");
        }
        if (PRIMARY_DELIM.equals(NULL_FLAG))
        { // should not reach here
          LOG.error("PRIMARY_DELIM must not equal with NULL_FLAG, PRIMARY_DELIM=["+PRIMARY_DELIM+
              "] NULL_FLAG=["+NULL_FLAG+"]");
        }
        if (rowKeyDesc == null) {
          rowKeyDesc = new RowKeyDesc();
          rowKeyDesc.buildRowKeyDesc((JobConf) getConf());
        }
        is_inited = true;
      }
    }

    public long getTaggedInteger(String str) {
      long rtval;
      try {
        rtval = Long.parseLong(str);
      } catch (NumberFormatException e) {
        rtval = 0;
        LOG.warn("Failed to parse integer [" + str + "]:" + e);
      }

      return rtval;
    }

    public Date getTaggedDate(String str) {
      SimpleDateFormat df = rowKeyDesc.getDateFromat(str);
      Date date = null;

      try {
        date = df.parse(str);
      } catch (ParseException e) {
        date = new Date();
        LOG.warn("Failed to parse date [" + str + "]: " + e);
      }

      return date;
    }

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      int n1 = WritableUtils.decodeVIntSize(b1[s1]);
      int n2 = WritableUtils.decodeVIntSize(b2[s2]);
      Text left = new Text(new String(b1, s1 + n1, l1 - n1));
      Text right = new Text(new String(b2, s2 + n2, l2 - n2));
      if (left == null || right == null) {
        LOG.warn("can't compare left[" + left + "] right[" + right + "]");
        return -1;
      } else {
        return compare(left, right);
      }
    }

    public int bytesCompare(byte[] left, byte[] right) {
      int i = 0;
      int len = left.length > right.length ? right.length : left.length;

      for (i = 0; i < len; ++i) {
        if (left[i] != right[i]) {
          if (left[i] < 0 && right[i] >= 0)
            return 1;
          else if (left[i] >= 0 && right[i] < 0)
            return -1;
          else
            return left[i] - right[i] > 0 ? 1 : -1;
        }
      }

      if (i == len) {
        if (left.length > right.length) {
          return 1;
        } else if (left.length == right.length) {
          return 0;
        } else {
          return -1;
        }
      }

      return 0;
    }

    @Override
    public int compare(Text left, Text right) {
      if (!is_inited) {
        init();
      }

      String[] left_columns = left.toString().split(PRIMARY_DELIM, -1);
      String[] right_columns = right.toString().split(PRIMARY_DELIM, -1);

      if (left_columns.length < rowKeyDesc.length || right_columns.length < rowKeyDesc.length) {
        String msg = "left_columns.length[" + left_columns.length + "] and" + " right_columns.length["
            + right_columns.length + "] should not " + " less than rowKeyDesc[" + rowKeyDesc.length + "]";
        LOG.warn(msg);
      }

      for (int i = 0; i < rowKeyDesc.length; ++i) {
        if (rowKeyDesc.getColumnType()[i] == rowKeyDesc.INT) {
          long left_val = getTaggedInteger(left_columns[i]);
          long right_val = getTaggedInteger(right_columns[i]);
          if (left_val != right_val) {
            if (left_val < right_val){
              return -1;
            }
            if (left_val > right_val){
              return 1;
            }
          }
        } else if (rowKeyDesc.getColumnType()[i] == rowKeyDesc.VARCHAR) {
          int ret = 0;
          try {
            ret = bytesCompare(left_columns[i].getBytes("UTF-8"), right_columns[i].getBytes("UTF-8"));
          } catch (UnsupportedEncodingException e) {
            String msg = "failed to decode string with UTF-8, left_column: " + left_columns[i] + " rigth_column: "
                + right_columns[i];
            LOG.error(msg);
          }
          if (0 != ret) {
            return ret;
          }
        } else if (rowKeyDesc.getColumnType()[i] == rowKeyDesc.PRECISE_DATETIME) {
          Date left_val = getTaggedDate(left_columns[i]);
          Date right_val = getTaggedDate(right_columns[i]);
          int ret = left_val.compareTo(right_val);
          if (0 != ret) {
            return ret;
          }
        }
      }

      return 0;
    }
  }

  /**
   * Partitioner effecting a total order by reading split points from an
   * externally generated source.
   */
  public static class TotalOrderPartitioner implements Partitioner<Text, Text> {

    private Text[] splitPoints;
    private Node<Text> partitions;
    private Node<Text> ranges;
    private ArrayList<Integer> partsPointsIdList = new ArrayList<Integer>();;

    public TotalOrderPartitioner() {
    }

    /**
     * Read in the partition file and build indexing data structures. Keys will
     * be located using a binary search of the partition keyset using the
     * {@link org.apache.hadoop.io.RawComparator} defined for this job. The
     * input file must be sorted with the same comparator and contain
     * {@link org.apache.hadoop.mapred.JobConf#getNumReduceTasks} - 1 keys.
     */
    @SuppressWarnings("unchecked")
    // keytype from conf not static
    public void configure(JobConf job) {
      try {
        final Path partFile = new Path(PARTITIOIN_FILE_NAME);
        final Path rangeFile = new Path(RANGE_FILE_NAME);
        final FileSystem fs = FileSystem.getLocal(job); // assume in DistributedCache
        PRIMARY_DELIM = getEscapeStr(job, "mrsstable.primary.delimeter", "\001");

        RawComparator<Text> comparator = (RawComparator<Text>) job.getOutputKeyComparator();
        String samplerType = job.get("mrsstable.presort.sampler", "specify");
        if (samplerType.equals("specify") && fs.exists(rangeFile)) {
          splitPoints = readPartitions(fs, rangeFile, job);
          Arrays.sort(splitPoints, comparator);
        } else if (!samplerType.equals("specify") && fs.exists(partFile)) {
          splitPoints = readPartitions(fs, partFile, job);
        } else {
          throw new IOException("partition file or range file don't exist " + "in distribute cache, sampler type: "
              + samplerType);
        }

        for (int i = 0; i < splitPoints.length - 1; ++i) {
          if (comparator.compare(splitPoints[i], splitPoints[i + 1]) >= 0) {
            String[] left = splitPoints[i].toString().split(PRIMARY_DELIM, -1);
            String[] right = splitPoints[i + 1].toString().split(PRIMARY_DELIM, -1);
            StringBuffer msg = new StringBuffer();
            msg.append("Split points are out of order, splitPoints[i]:");
            for (String s: left){
              msg.append(s);
              msg.append(" ");
            }
            msg.append("splitPoints[i + 1]:");
            for (String s: right){
              msg.append(s);
              msg.append(" ");
            }
            throw new IOException(msg.toString());
          }
        }

        int reduceNum = job.getNumReduceTasks();
        if (splitPoints.length > reduceNum - 1) {
          float stepSize = splitPoints.length / (float) reduceNum;
          int last = 0;
          ArrayList<Text> partsPointsList = new ArrayList<Text>();
          for (int i = 1; i < reduceNum; ++i) {
            int k = Math.round(stepSize * i);
            while (comparator.compare(splitPoints[last], splitPoints[k]) >= 0) {
              ++k;
            }
            partsPointsList.add(splitPoints[k]);
            partsPointsIdList.add(k);
            last = k;
          }
          Text[] partsPoints = partsPointsList.toArray(new Text[partsPointsList.size()]);
          partitions = new BinarySearchNode(partsPoints, comparator);
        } else {
          partitions = new BinarySearchNode(splitPoints, comparator);
          for (int k = 0; k < splitPoints.length; ++k) {
            partsPointsIdList.add(k);
          }
        }
        ranges = new BinarySearchNode(splitPoints, comparator);
      } catch (IOException e) {
        throw new IllegalArgumentException("Can't read partitions file", e);
      }
    }

    public int getPartition(Text key, Text value, int numPartitions) {
      return partitions.findPartition(key);
    }

    public int getRange(Text key) {
      return ranges.findPartition(key);
    }

    public List<Integer> getRange(int patition) {
      List<Integer> ranges = new ArrayList<Integer>();
      int start = -1;
      int end = -2;
      if (patition == 0) {
        start = 0;
        end = partsPointsIdList.get(0);
      } else if (patition < partsPointsIdList.size()) {
        start = partsPointsIdList.get(patition - 1) + 1;
        end = partsPointsIdList.get(patition);
      } else if (patition == partsPointsIdList.size()) {
        start = partsPointsIdList.get(patition - 1) + 1;
        end = splitPoints.length;
      } else {
        LOG.info("no ranges found for patition id[" + patition + "]");
      }

      while (start <= end) {
        ranges.add(start++);
      }

      return ranges;
    }

    public Text getRangeEndKey(int index) {
      return ranges.getTextKey(index);
    }

    public int getRangeNum() {
      return ranges.getPartitionNum();
    }

    /**
     * Set the path to the SequenceFile storing the sorted partition keyset. It
     * must be the case that for <tt>R</tt> reduces, there are <tt>R-1</tt> keys
     * in the SequenceFile.
     */
    public static void setPartitionFile(JobConf job, Path p) {
      job.set("total.order.partitioner.path", p.toString());
    }

    /**
     * Get the path to the SequenceFile storing the sorted partition keyset.
     * @throws IOException
     *
     * @see #setPartitionFile(JobConf,Path)
     */
    public static String getPartitionFile(JobConf job) throws IOException {
      return job.get("total.order.partitioner.path", PARTITIOIN_FILE_NAME);
    }

    /**
     * Interface to the partitioner to locate a key in the partition keyset.
     */
    interface Node<T> {
      /**
       * Locate partition in keyset K, st [Ki..Ki+1) defines a partition, with
       * implicit K0 = -inf, Kn = +inf, and |K| = #partitions - 1.
       */
      int findPartition(T key);

      Text getTextKey(int index);

      int getPartitionNum();
    }

    /**
     * For types that are not {@link org.apache.hadoop.io.BinaryComparable} or
     * where disabled by <tt>total.order.partitioner.natural.order</tt>, search
     * the partition keyset with a binary search.
     */
    class BinarySearchNode implements Node<Text> {
      private final Text[] splitPoints;
      private final RawComparator<Text> comparator;

      BinarySearchNode(Text[] splitPoints, RawComparator<Text> comparator) {
        this.splitPoints = splitPoints;
        this.comparator = comparator;
      }

      public int findPartition(Text key) {
        final int pos = Arrays.binarySearch(splitPoints, key, comparator) + 1;
        return (pos <= 0) ? -pos : pos - 1;
      }

      public Text getTextKey(int index) {
        if (index >= 0 && index < splitPoints.length) {
          return splitPoints[index];
        } else {
          throw new IllegalArgumentException("invalid range index" + index);
        }
      }

      public int getPartitionNum() {
        return splitPoints.length + 1;
      }
    }

    /**
     * Read the cut points from the given IFile.
     * 
     * @param fs
     *          The file system
     * @param p
     *          The path to read
     * @param job
     *          The job config
     * @throws IOException
     */
    // matching key types enforced by passing in
    // map output key class
    private Text[] readPartitions(FileSystem fs, Path p, JobConf job) throws IOException {
      FileStatus[] fileStatus = fs.listStatus(p);
      FSDataInputStream fileIn = fs.open(p);
      RecordReader<LongWritable, Text> reader = new LineRecordReader(fileIn, 0, fileStatus[0].getLen(), job);
      LongWritable key = reader.createKey();
      Text value = reader.createValue();
      ArrayList<Text> parts = new ArrayList<Text>();
      while (reader.next(key, value)) {
        String rowkey = value.toString();
        parts.add(new Text(rowkey));
        key = reader.createKey();
      }
      reader.close();
      return parts.toArray(new Text[parts.size()]);
    }
  }

  private void initPartitioner(JobConf conf) throws IOException, URISyntaxException {
    // Set paramters
    PRIMARY_DELIM = getEscapeStr(conf, "mrsstable.primary.delimeter", "\001");
    NULL_FLAG = getEscapeStr(conf, "mrsstable.primary.nullflag", "\002");

    if (PRIMARY_DELIM.equals(NULL_FLAG))
    {
      throw new IOException("PRIMARY_DELIM must not equal with NULL_FLAG, PRIMARY_DELIM=["+PRIMARY_DELIM+
          "] NULL_FLAG=["+NULL_FLAG+"]");
    }

    boolean isSkipInvalidRow = false;
    String skipInvalidRow = conf.get("mrsstable.skip.invalid.row", "1");
    if (skipInvalidRow.equalsIgnoreCase("1")) {
      isSkipInvalidRow = true;
    }
    RowKeyDesc rowKeyDesc = new RowKeyDesc();
    rowKeyDesc.buildRowKeyDesc(conf);

    // Sample original files
    conf.setPartitionerClass(TotalOrderPartitioner.class);

    int reduceNum = conf.getNumReduceTasks();
    TextInputSampler.Sampler sampler = null;
    String samplerType = conf.get("mrsstable.presort.sampler", "specify");

    if (samplerType.equals("split")) {
      LOG.info("Using split sampler...");
      String defaultNumSampleStr = Integer.toString(reduceNum * 10);
      String numSampleStr = conf.get("mrsstable.sample.number", defaultNumSampleStr);
      int numSample = Integer.parseInt(numSampleStr);
      String defaultMaxSplitStr = Integer.toString(reduceNum / 5);
      String maxSplitStr = conf.get("mrsstable.max.sample.split", defaultMaxSplitStr);
      int maxSampleSplit = Integer.parseInt(maxSplitStr);
      sampler = new TextInputSampler.SplitSampler(numSample, maxSampleSplit, rowKeyDesc,
          PRIMARY_DELIM, isSkipInvalidRow);
      writePartsFile = true;
    } else if (samplerType.equals("interval")) {
      LOG.info("Using interval sampler...");
      String defaultChoosePercentpStr = Double.toString(0.001);
      String choosePercentStr = conf.get("mrsstable.sample.choose.percent", defaultChoosePercentpStr);
      double choosePercent = Double.parseDouble(choosePercentStr);
      int maxSplits = (int) Math.max(reduceNum * 0.05, 1);
      String defaultMaxSplitStr = Integer.toString(maxSplits);
      String maxSplitStr = conf.get("mrsstable.max.sample.split", defaultMaxSplitStr);
      int maxSampleSplit = Integer.parseInt(maxSplitStr);
      sampler = new TextInputSampler.IntervalSampler(choosePercent, maxSampleSplit, rowKeyDesc,
          PRIMARY_DELIM, isSkipInvalidRow);
      writePartsFile = true;
    } else if (samplerType.equals("random")) {
      LOG.info("Using random sampler...");
      String defaultChoosePercentpStr = Double.toString(0.001);
      String choosePercentStr = conf.get("mrsstable.sample.choose.percent", defaultChoosePercentpStr);
      double choosePercent = Double.parseDouble(choosePercentStr);
      String defaultNumSampleStr = Integer.toString(reduceNum * 10);
      String numSampleStr = conf.get("mrsstable.sample.number", defaultNumSampleStr);
      int numSample = Integer.parseInt(numSampleStr);
      String defaultMaxSplitStr = Integer.toString(reduceNum / 5);
      String maxSplitStr = conf.get("mrsstable.max.sample.split", defaultMaxSplitStr);
      int maxSampleSplit = Integer.parseInt(maxSplitStr);
      sampler = new TextInputSampler.RandomSampler(choosePercent, numSample, maxSampleSplit,
          rowKeyDesc, PRIMARY_DELIM, isSkipInvalidRow);
      writePartsFile = true;

    } else {
      // user specify range list
      LOG.info("User specify range list...");
      TotalOrderPartitioner.setPartitionFile(conf, new Path(RANGE_FILE_NAME));
    }


    String[] archivesStr = conf.get("tmparchives", "").split(",", -1);
    if (archivesStr.length == 0 || archivesStr[0].equals("")) {
      throw new IllegalArgumentException("not specify config archive file");
    } else {
      URI configArchiveUri = new URI(archivesStr[0] + "#" + CONFIG_DIR);
      DistributedCache.addCacheArchive(configArchiveUri, conf);
    }

    if (writePartsFile) {
      Path input = FileInputFormat.getInputPaths(conf)[0];
      FileSystem fs = input.getFileSystem(conf);
      input = input.makeQualified(input.getFileSystem(conf));
      Path partitionFile = new Path(input, PARTITIOIN_FILE_NAME);
      if (!conf.get("mrsstable.partition.file", "").equals("")) {
        SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyyMMddhhmmssSSS");
        partitionFile = new Path(conf.get("mrsstable.partition.file", "") + "_" + dateFormatter.format(new Date()));
        LOG.info("Partition Filename is " + partitionFile.toString());
      }

      TotalOrderPartitioner.setPartitionFile(conf, partitionFile);
      TextInputSampler.writePartitionFile(conf, sampler);

      // Add to DistributedCache
      URI partitionUri = new URI(partitionFile.toString() + "#" + PARTITIOIN_FILE_NAME);
      DistributedCache.addCacheFile(partitionUri, conf);
    }
  }

  private void mvPartitionFile(JobConf conf) throws IOException {
    Path partitionFile = new Path(TotalOrderPartitioner.getPartitionFile(conf));
    Path outputDir = FileOutputFormat.getOutputPath(conf);
    Path newPartitionFile = new Path(outputDir, PARTITIOIN_FILE_NAME);
    FileSystem fs = partitionFile.getFileSystem(conf);
    if (!fs.rename(partitionFile, newPartitionFile)) {
      String msg = "failed mv Partition File from " + partitionFile.toString() + " to " + newPartitionFile.toString();
      LOG.error(msg);
      throw new IOException(msg);
    }
    else{
      LOG.info("mv Partition File from " + partitionFile.toString() + " to " + newPartitionFile.toString());
    }
  }
  
  private void writeTableInfo(JobConf conf) throws IOException {
    Path outputDir = FileOutputFormat.getOutputPath(conf);
    Path tableInfoFile = new Path(outputDir, TABLE_INFO_FILE);
    LOG.info("write table info file: " + tableInfoFile.toString());
    FileSystem fs = tableInfoFile.getFileSystem(conf);
    if (fs.exists(tableInfoFile)) {
      String msg = tableInfoFile.toString() + " should not exist!";
      LOG.error(msg);
      throw new IOException(msg);
    }
    
    FSDataOutputStream fileOut = fs.create(tableInfoFile);
    
    // write table name
    String table_name = "table_name=" + conf.get("mrsstable.table.name", "0") + "\n";
    fileOut.write(table_name.getBytes("UTF-8"));
    
    // write table id
    String table_id = "table_id=" + conf.get("mrsstable.table.id", "0") + "\n";
    fileOut.write(table_id.getBytes("UTF-8"));    
    
    // write rowkey desc
    RowKeyDesc rowKeyDesc;
    rowKeyDesc = new RowKeyDesc();
    rowKeyDesc.buildRowKeyDesc(conf);
    String rowkeyDescStr = "rowkey_desc=" + rowKeyDesc.getSimpleDesc() + "\n";
    fileOut.write(rowkeyDescStr.getBytes("UTF-8"));    
    
    // sstable_version
    String sstable_verion = "sstable_version=" + conf.get("mrsstable.sstable.version", "-1") + "\n";
    fileOut.write(sstable_verion.getBytes("UTF-8"));
    
    // write delim
    String delimStr = getEscapeStr(conf, "mrsstable.primary.delimeter", "\001");
    if (delimStr.length() != 1){
      throw new IllegalArgumentException("delim length["+delimStr.length()+"] must = 1");
    }
    char delimChar = delimStr.charAt(0);
      
    String delim = "delim=" + (int)delimChar + "\n";
    fileOut.write(delim.getBytes("UTF-8"));
        
    fileOut.close();
  }

  private void initDistributeCache(JobConf conf) throws URISyntaxException {
    Path nativeLibPath;
    String nativeLibStr = conf.get("mrsstable.native.lib.path", "");
    if (nativeLibStr != "") {
      nativeLibPath = new Path(conf.get("mrsstable.native.lib.path", ""));
    } else {
      throw new IllegalArgumentException("not specify native lib path");
    }

    // Add libnone to DistributedCache
    Path libNoneFile = new Path(nativeLibPath, LIB_NONE_OB_NAME);
    if (conf.get("mrsstable.libnone.ob.file", "") != "") {
      libNoneFile = new Path(conf.get("mrsstable.libnone.ob.file", ""));
    }
    URI libNoneUri = new URI(libNoneFile.toString() + "#" + LIB_NONE_OB_NAME);
    DistributedCache.addCacheFile(libNoneUri, conf);

    // Add org liblzo to DistributedCache
    Path liblzoOrgFile = new Path(nativeLibPath, LIB_LZO_ORG_NAME);
    if (conf.get("mrsstable.liblzo.org.file", "") != "") {
      liblzoOrgFile = new Path(conf.get("mrsstable.liblzo.org.file", ""));
    }
    URI liblzoOrgUri = new URI(liblzoOrgFile.toString() + "#" + LIB_LZO_ORG_NAME);
    DistributedCache.addCacheFile(liblzoOrgUri, conf);

    // Add ob liblzo to DistributedCache
    Path liblzoObFile = new Path(nativeLibPath, LIB_LZO_OB_NAME);
    if (conf.get("mrsstable.liblzo.ob.file", "") != "") {
      liblzoObFile = new Path(conf.get("mrsstable.liblzo.ob.file", ""));
    }
    URI liblzoObUri = new URI(liblzoObFile.toString() + "#" + LIB_LZO_OB_NAME);
    DistributedCache.addCacheFile(liblzoObUri, conf);

    // Add org libsnappy to DistributedCache
    Path libsnappyOrgFile = new Path(nativeLibPath, LIB_SNAPPY_ORG_NAME);
    if (conf.get("mrsstable.libsnappy.org.file", "") != "") {
      libsnappyOrgFile = new Path(conf.get("mrsstable.libsnappy.org.file", ""));
    }
    URI libsnappyOrgUri = new URI(libsnappyOrgFile.toString() + "#" + LIB_SNAPPY_ORG_NAME);
    DistributedCache.addCacheFile(libsnappyOrgUri, conf);

    // Add ob libsnappy to DistributedCache
    Path libsnappyObFile = new Path(nativeLibPath, LIB_SNAPPY_OB_NAME);
    if (conf.get("mrsstable.libsnappy.ob.file", "") != "") {
      libsnappyObFile = new Path(conf.get("mrsstable.libsnappy.ob.file", ""));
    }
    URI libsnappyObUri = new URI(libsnappyObFile.toString() + "#" + LIB_SNAPPY_OB_NAME);
    DistributedCache.addCacheFile(libsnappyObUri, conf);

    // Add libmrsstable to DistributedCache
    Path mrsstableFile = new Path(nativeLibPath, LIB_MRSSTABLE_NAME);
    if (conf.get("mrsstable.jni.so.file", "") != "") {
      mrsstableFile = new Path(conf.get("mrsstable.jni.so.file", ""));
    }
    URI mrsstableUri = new URI(mrsstableFile.toString() + "#" + LIB_MRSSTABLE_NAME);
    DistributedCache.addCacheFile(mrsstableUri, conf);
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.printf("args' count is not two: ");
      for (String arg : args) {
        System.err.printf(" " + arg);
      }
      System.err.printf("\n");
      MrsstableRunner.printMrsstableCommandUsage(System.err);
      return -1;
    }

    JobConf conf = new JobConf(getConf(), MRGenSstable.class);
    FileInputFormat.setInputPaths(conf, args[0]);
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    String reduceNumStr = conf.get("mapred.reduce.tasks", "700");
    conf.setNumReduceTasks(Integer.parseInt(reduceNumStr));
    LOG.info("Reduce num:" + conf.getNumReduceTasks());

    String inputFormat = conf.get("mrsstable.input.format", "text");
    if (inputFormat.equals("text")) {
      LOG.info("Use text input format");
      conf.setInputFormat(TextInputFormat.class);
    } else if (inputFormat.equals("sequence")) {
      LOG.info("Use sequence input format");
      conf.setInputFormat(SequenceFileAsTextInputFormat.class);
    } else {
      System.err.printf("Unsupport input format: " + inputFormat);
      return -1;
    }
    
    String table_name = new Path(args[1]).getName();
    String app_name = new Path(args[1]).getParent().getParent().getName();

    conf.setOutputFormat(SSTableOutputformat.class);
    conf.setOutputKeyClass(Text.class);
    conf.setMapperClass(TokenizerMapper.class);
    conf.setReducerClass(SSTableReducer.class);
    conf.setOutputKeyComparatorClass(TaggedKeyComparator.class);
    
    if (conf.getJobName().isEmpty()) {
      String jobName = conf.get("mrsstable.jobname", "mrsstable");
      conf.setJobName(jobName);
    }
    String samplerType = conf.get("mrsstable.presort.sampler", "specify");
    if (!samplerType.equals("specify")){
      String path = conf.get("mrsstable.partition.file", "");
      if(path.isEmpty()){
        System.err.printf("mrsstable.partition.file is not set");
        return -1;
      }
      else
      {
        path += app_name + "_" + table_name;
        conf.set("mrsstable.partition.file", path);        
      }
    }
    initDistributeCache(conf);
    initPartitioner(conf);
    DistributedCache.createSymlink(conf);
    RunningJob job = JobClient.runJob(conf);
    mvPartitionFile(conf);
    writeTableInfo(conf);

    if (job.getJobState() != JobStatus.SUCCEEDED) {
      return -1;
    } else {
      return 0;
    }
  }
}
