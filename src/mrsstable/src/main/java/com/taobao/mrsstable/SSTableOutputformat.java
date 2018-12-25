package com.taobao.mrsstable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

import com.taobao.mrsstable.MRGenSstable.TotalOrderPartitioner;

public class SSTableOutputformat extends FileOutputFormat<Text, Text> implements Configurable {

  private JobConf jobConf = null;
  private String baseName = null;
  private static final MRLogger LOG = MRLogger.getLogger(SSTableOutputformat.class);

  protected class SSTableWriter implements RecordWriter<Text, Text> {

    private static final String LIB_SSTABLE_NAME = "mrsstable";
    private static final String LIB_NONE_OB_NAME = "none";
    private static final String LIB_LZO_ORG_NAME = "lzo2";
    private static final String LIB_LZO_OB_NAME = "lzo_1.0";
    private static final String LIB_SNAPPY_ORG_NAME = "snappy";
    private static final String LIB_SNAPPY_OB_NAME = "snappy_1.0";
    private static final String SCHEMA_FILE_NAME = "config/schema.ini";
    private static final String DATA_SYNTAX_FILE_NAME = "config/data_syntax.ini";
    private static final int DIRECT_BUF_SIZE = 2 * 1024 * 1024;

    protected DataOutputStream out;
    private TotalOrderPartitioner partitioner;
    private SSTableBuilder sstableBuilder;
    private ByteBuffer byteBuffer;
    private String fileNameSeq = "0000000000"; // 10 * '0'
    private int prev_range_no = -1;
    private boolean is_first = false;
    private boolean is_include_min = false;
    private boolean is_include_max = false;
    private boolean is_inited = false;
    private String tableId = null;
    private String rowkeyDesc = null;
    private JobConf job = null;
    private List<Integer> ranges = null;
    private boolean isSkipInvalidRow = true;

    private static final String utf8 = "UTF-8";
    private byte[] newline;

    public SSTableWriter(JobConf job) {
      this.job = job;
      String partitionIdStr = this.job.get("mapred.task.partition");
      Integer partitionId = Integer.parseInt(partitionIdStr);
      LOG.info("partitionId: " + partitionIdStr);

      this.tableId = jobConf.get("mrsstable.table.id", "");
      if (this.tableId.equals("")) {
        throw new IllegalArgumentException("not specify table id");
      }

      this.rowkeyDesc = jobConf.get("mrsstable.rowkey.desc", "");
      if (rowkeyDesc.equals("")) {
        throw new IllegalArgumentException("not specify rowkey desc");
      }

      String isSkipInvalidRowStr = jobConf.get("mrsstable.skip.invalid.row", "0");
      if (isSkipInvalidRowStr.equals("0")) {
        this.isSkipInvalidRow = false;
      } else if (isSkipInvalidRowStr.equals("1")) {
        this.isSkipInvalidRow = true;
      } else {
        throw new IllegalArgumentException("not specify mrsstable.skip.invalid.row");
      }

      partitioner = new TotalOrderPartitioner();
      partitioner.configure(jobConf);
      ranges = partitioner.getRange(partitionId);
      StringBuffer msg = new StringBuffer();
      for (int range : ranges) {
        msg.append(range);
        msg.append(" ");
      }
      LOG.info("ranges: " + msg.toString());
    }

    private void init() throws IOException {
      if (!is_inited) {
        try {
          System.loadLibrary(LIB_LZO_ORG_NAME);
          System.loadLibrary(LIB_LZO_OB_NAME);
          System.loadLibrary(LIB_SNAPPY_ORG_NAME);
          System.loadLibrary(LIB_SNAPPY_OB_NAME);
          System.loadLibrary(LIB_NONE_OB_NAME);
          System.loadLibrary(LIB_SSTABLE_NAME);
        } catch (UnsatisfiedLinkError e) {
          throw new UnsatisfiedLinkError("Cannot load sstable library:\n " + e.toString());
        }

        try {
          newline = "\n".getBytes(utf8);
        } catch (UnsupportedEncodingException uee) {
          throw new IllegalArgumentException("can't find " + utf8 + " encoding");
        }

        sstableBuilder = new SSTableBuilder();
        int err = sstableBuilder.init(SCHEMA_FILE_NAME, DATA_SYNTAX_FILE_NAME, this.tableId, this.isSkipInvalidRow);
        if (err != 0) {
          LOG.error("init sstable builder failed");
          throw new IllegalArgumentException("can't init sstable builder");
        }
        try {
          byteBuffer = ByteBuffer.allocateDirect(DIRECT_BUF_SIZE);
        } catch (OutOfMemoryError e) {
          LOG.error("Faild to allocate byte buffer for SSTableOutputformat: " + e);
          throw e;
        }
        byteBuffer.clear();
        prev_range_no = -1;
        is_inited = true;
      }
    }

    private String generateFileName(String name, int range_no) {
      StringBuilder strBuilder = new StringBuilder(256);
      strBuilder.append(tableId).append("-").append(new DecimalFormat(fileNameSeq).format(range_no));
      return strBuilder.toString();
    }

    private void createEmptySstable(int range_no) throws IOException {
      LOG.info("create an empty sstable of range: " + range_no);
      createNewSSTableFile(range_no);
      writeCurrentByteBuffer(true);
      out.close();
    }

    private void createNewSSTableFile(int range_no) throws IOException {
      if (!is_inited) {
        init();
      }
      if (ranges.remove((Integer) range_no)) {
        LOG.info("create a sstable of range: " + range_no);
      } else {
        LOG.error("this reducer has created or should not create a sstable of range: " + range_no);
      }
      String name = generateFileName(baseName, range_no);
      Path file = FileOutputFormat.getTaskOutputPath(jobConf, name);
      FileSystem fs = file.getFileSystem(jobConf);
      out = fs.create(file);
      is_first = true;

      if (range_no == 0) {
        is_include_min = true;
        Text endKey = partitioner.getRangeEndKey(range_no);
        byteBuffer.put(endKey.getBytes(), 0, endKey.getLength());
        byteBuffer.put(newline);
      } else if (range_no == partitioner.getRangeNum() - 1) {
        is_include_max = true;
        Text startKey = partitioner.getRangeEndKey(partitioner.getRangeNum() - 2);
        byteBuffer.put(startKey.getBytes(), 0, startKey.getLength());
        byteBuffer.put(newline);
      } else {
        Text startKey = partitioner.getRangeEndKey(range_no - 1);
        byteBuffer.put(startKey.getBytes(), 0, startKey.getLength());
        byteBuffer.put(newline);
        Text endKey = partitioner.getRangeEndKey(range_no);
        byteBuffer.put(endKey.getBytes(), 0, endKey.getLength());
        byteBuffer.put(newline);
      }
    }

    private void writeCurrentByteBuffer(boolean is_last) throws IOException {
      byteBuffer.flip();
      ByteBuffer tmpByteBuf = byteBuffer.slice();
      ByteBuffer output = sstableBuilder.append(tmpByteBuf, is_first, is_last, is_include_min, is_include_max);
      if (output != null) {
        byte[] reslutBuf = new byte[output.capacity()];
        output.get(reslutBuf);
        out.write(reslutBuf);
      } else {
        LOG.error("failed to append sstable builder");
        throw new IOException("failed to append data to sstable");
      }
      byteBuffer.clear();
      is_first = false;
      is_include_min = false;
      is_include_max = false;
    }

    public synchronized void write(Text key, Text value) throws IOException {
      if (!is_inited) {
        init();
      }

      int cur_range_no = partitioner.getRange(key);
      if (cur_range_no < 0) {
        LOG.error("invalid range number" + cur_range_no);
        return;
      } else if (value.getLength() > DIRECT_BUF_SIZE) {
        LOG.error("value is too large to append" + value.getLength());
        return;
      }

      if (prev_range_no == -1) {
        createNewSSTableFile(cur_range_no);
      }

      if (prev_range_no != -1 && cur_range_no >= 0 && cur_range_no != prev_range_no) {
        writeCurrentByteBuffer(true);
        out.close();
        createNewSSTableFile(cur_range_no);
      } else if (byteBuffer.remaining() < value.getLength() + newline.length) {
        writeCurrentByteBuffer(false);
      }

      byteBuffer.put(value.getBytes(), 0, value.getLength());
      byteBuffer.put(newline);
      prev_range_no = cur_range_no;
    }

    public synchronized void close(Reporter reporter) throws IOException {
      if (is_inited) {
        writeCurrentByteBuffer(true);
        out.close();
      } else {
        LOG.warn("SSTableWriter is not inited yet");
      }

      if (!ranges.isEmpty()) {
        Integer[] emptyRanges = ranges.toArray(new Integer[ranges.size()]);
        for (Integer range : emptyRanges) {
          createEmptySstable(range);
        }
      }

      if (is_inited) {
        this.sstableBuilder.close();
      }

    }
  }

  @Override
  public void setConf(Configuration conf) {
    jobConf = (JobConf) conf;
  }

  @Override
  public Configuration getConf() {
    return jobConf;
  }

  @Override
  public RecordWriter<Text, Text> getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress)
      throws IOException {
    baseName = new String(name);
    return new SSTableWriter(job);
  }
}
