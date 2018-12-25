package com.taobao.mrsstable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.InputSampler;

import com.taobao.mrsstable.MRGenSstable.RowKeyDesc;
import com.taobao.mrsstable.MRGenSstable.TotalOrderPartitioner;

public class TextInputSampler extends InputSampler<Writable, Text> {
  public TextInputSampler(JobConf conf) {
    super(conf);
  }

  /**
   * Interface to sample using an {@link org.apache.hadoop.mapred.InputFormat} .
   */
  public interface Sampler {
    /**
     * For a given job, collect and return a subset of the keys from the input
     * data.
     */
    Text[] getSample(InputFormat<Writable, Text> inf, JobConf job) throws IOException;
  }

  /**
   * Sample from random points in the input. General-purpose sampler. Takes
   * numSamples / maxSplitsSampled inputs from each split.
   */
  public static class RandomSampler implements Sampler {
    private final int numSamples;
    private final int maxSplitsSampled;
    private final RowKeyDesc rowKeyDesc;
    private final String delimiter;
    private final boolean isSkipInvalidRow;


    /**
     * Create a new RandomSampler sampling <em>all</em> splits. This will read
     * every split at the client, which is very expensive.
     *
     * @param freq
     *          Probability with which a key will be chosen.
     * @param numSamples
     *          Total number of samples to obtain from all selected splits.
     */
    public RandomSampler(double freq, int numSamples, RowKeyDesc rowKeyDesc, String delimiter, boolean isSkipInvalidRow) {
      this(freq, numSamples, Integer.MAX_VALUE, rowKeyDesc, delimiter, isSkipInvalidRow);
    }

    /**
     * Create a new RandomSampler.
     *
     * @param numSamples
     *          Total number of samples to obtain from all selected splits.
     * @param maxSplitsSampled
     *          The maximum number of splits to examine.
     */
    public RandomSampler(double freq, int numSamples, int maxSplitsSampled, RowKeyDesc rowKeyDesc, String delimiter, boolean isSkipInvalidRow) {
      this.numSamples = numSamples;
      this.maxSplitsSampled = maxSplitsSampled;
      this.rowKeyDesc = rowKeyDesc;
      this.isSkipInvalidRow = isSkipInvalidRow;
      this.delimiter = delimiter;
    }

    /**
     * Randomize the split order, then take the specified number of keys from
     * each split sampled, where each key is selected with the equal probability.
     */
    public Text[] getSample(InputFormat<Writable, Text> inf, JobConf job) throws IOException {
      InputSplit[] splits = inf.getSplits(job, job.getNumMapTasks());
      int numPartitions = job.getNumReduceTasks();
      ArrayList<Text> samples = new ArrayList<Text>(numSamples);
      if (splits.length == 0) {
        throw new IOException("no input files, split length:" + splits.length);
      }
      else if (numSamples < numPartitions)
      {
        throw new IOException("numSamples=" + numSamples + " must not less than numPartitions=" + numPartitions);
      }
      int splitsToSample = Math.min(maxSplitsSampled, splits.length);
      System.err.println("splitsToSample="+splitsToSample+" splits.length="+splits.length);
      long total_line = 0;

      Random r = new Random();
      long seed = r.nextLong();
      r.setSeed(seed);
      // shuffle splits
      for (int i = 0; i < splits.length; ++i) {
        InputSplit tmp = splits[i];
        int j = r.nextInt(splits.length);
        splits[i] = splits[j];
        splits[j] = tmp;
      }
      // select key from splits with the equal probability
      for (int i = 0; i < splitsToSample || (samples.size() < numPartitions && i < splits.length); ++i) {
        System.err.println("sample split: " + i);
        RecordReader<Writable, Text> reader = inf.getRecordReader(splits[i], job, Reporter.NULL);
        Writable key = reader.createKey();
        Text value = reader.createValue();
        while (reader.next(key, value)) {
          ++total_line;
          if (samples.size() < numSamples || r.nextDouble() <= 1.0 * numSamples / total_line) {
            Text t = null;
            try {
              t = getDispatchKey(value, delimiter, rowKeyDesc);
            } catch (IllegalArgumentException e) {
              if (this.isSkipInvalidRow)
              {
                System.out.println("skip invalid rowkey: " + e.getMessage());
                continue;
              }
              else
              {
                throw e;
              }
            }

            if (samples.size() < numSamples) {
              //System.out.println("Added key:" + t.toString().replace(delimiter, " "));
              samples.add(t);
            } else {
              // When exceeding the maximum number of samples, replace a
              // random element with this one
              int ind = r.nextInt(numSamples);
              if (ind != numSamples) {
                //System.out.println("Replace key:" + t.toString().replace(delimiter, " "));
                samples.set(ind, t);
              }
            }
            key = reader.createKey();
          }
        }
        reader.close();
      }
      return samples.toArray(new Text[samples.size()]);
    }
  }

  /**
   * Sample from s splits at regular intervals. Useful for sorted data.
   */
  public static class IntervalSampler implements Sampler {
    private final double freq;
    private final int maxSplitsSampled;
    private final RowKeyDesc rowKeyDesc;
    private final String delimiter;
    private final boolean isSkipInvalidRow;

    /**
     * Create a new IntervalSampler sampling <em>all</em> splits.
     *
     * @param freq
     *          The frequency with which records will be emitted.
     */
    public IntervalSampler(double freq, RowKeyDesc rowKeyDesc, String delimiter, boolean isSkipInvalidRow) {
      this(freq, Integer.MAX_VALUE, rowKeyDesc, delimiter, isSkipInvalidRow);
    }

    /**
     * Create a new IntervalSampler.
     *
     * @param freq
     *          The frequency with which records will be emitted.
     * @param maxSplitsSampled
     *          The maximum number of splits to examine.
     * @see #getSample
     */
    public IntervalSampler(double freq, int maxSplitsSampled, RowKeyDesc rowKeyDesc, String delimiter, boolean isSkipInvalidRow) {
      this.freq = freq;
      this.maxSplitsSampled = maxSplitsSampled;
      this.rowKeyDesc = rowKeyDesc;
      this.delimiter = delimiter;
      this.isSkipInvalidRow = isSkipInvalidRow;
    }

    /**
     * For each split sampled, emit when the ratio of the number of records
     * retained to the total record count is less than the specified frequency.
     */
    // ArrayList::toArray doesn't preserve type
    public Text[] getSample(InputFormat<Writable, Text> inf, JobConf job) throws IOException {
      InputSplit[] splits = inf.getSplits(job, job.getNumMapTasks());
      ArrayList<Text> samples = new ArrayList<Text>();
      if (splits.length == 0) {
        throw new IOException("no input files, split length:" + splits.length);
      }
      int splitsToSample = Math.min(maxSplitsSampled, splits.length);
      int splitStep = splits.length / splitsToSample;
      long records = 0;
      long kept = 0;
      for (int i = 0; i < splitsToSample; ++i) {
        RecordReader<Writable, Text> reader = inf.getRecordReader(splits[i * splitStep], job, Reporter.NULL);
        Writable key = reader.createKey();
        Text value = reader.createValue();
        while (reader.next(key, value)) {
          ++records;
          if ((double) kept / records < freq) {
            Text t = null;
            try {
              t = getDispatchKey(value, delimiter, rowKeyDesc);
            } catch (IllegalArgumentException e) {
              if (isSkipInvalidRow)
              {
                System.out.println("skip invalid rowkey: " + e.getMessage());
                continue;
              }
              else
              {
                throw e;
              }
            }

            ++kept;
            // System.out.println("Added key:"
            // + t.toString().replace(delimiter, " "));
            samples.add(t);
            key = reader.createKey();
          }
        }
        reader.close();
      }
      return samples.toArray(new Text[samples.size()]);
    }
  }

  /**
   * Samples the first n records from s splits. Inexpensive way to sample random
   * data.
   */
  public static class SplitSampler implements Sampler {

    private final int numSamples;
    private final int maxSplitsSampled;
    private final RowKeyDesc rowKeyDesc;
    private final String delimiter;
    private final boolean isSkipInvalidRow;

    /**
     * Create a SplitSampler sampling <em>all</em> splits. Takes the first
     * numSamples / numSplits records from each split.
     *
     * @param numSamples
     *          Total number of samples to obtain from all selected splits.
     */
    public SplitSampler(int numSamples, RowKeyDesc rowKeyDesc, String delimiter, boolean isSkipInvalidRow) {
      this(numSamples, Integer.MAX_VALUE, rowKeyDesc, delimiter, isSkipInvalidRow);
    }

    /**
     * Create a new SplitSampler.
     *
     * @param numSamples
     *          Total number of samples to obtain from all selected splits.
     * @param maxSplitsSampled
     *          The maximum number of splits to examine.
     */
    public SplitSampler(int numSamples, int maxSplitsSampled, RowKeyDesc rowKeyDesc, String delimiter, boolean isSkipInvalidRow) {
      this.numSamples = numSamples;
      this.maxSplitsSampled = maxSplitsSampled;
      this.rowKeyDesc = rowKeyDesc;
      this.delimiter = delimiter;
      this.isSkipInvalidRow = isSkipInvalidRow;
    }

    /**
     * From each split sampled, take the first numSamples / numSplits records.
     */
    public Text[] getSample(InputFormat<Writable, Text> inf, JobConf job) throws IOException {
      InputSplit[] splits = inf.getSplits(job, job.getNumMapTasks());
      ArrayList<Text> samples = new ArrayList<Text>(numSamples);
      if (splits.length == 0) {
        throw new IOException("no input files, split length:" + splits.length);
      }
      int splitsToSample = Math.min(maxSplitsSampled, splits.length);
      int splitStep = splits.length / splitsToSample;
      int samplesPerSplit = numSamples / splitsToSample;
      System.out.println("splitStep:" + splitStep + ", and splits:" + splits.length);
      System.out.println("samplesPerSplit:" + samplesPerSplit);
      long records = 0;
      for (int i = 0; i < splitsToSample; ++i) {
        RecordReader<Writable, Text> reader = inf.getRecordReader(splits[i * splitStep], job, Reporter.NULL);
        Writable key = reader.createKey();
        Text value = reader.createValue();
        while (reader.next(key, value)) {
          Text t = null;
          try {
            t = getDispatchKey(value, delimiter, rowKeyDesc);
          } catch (IllegalArgumentException e) {
            if (isSkipInvalidRow)
            {
              System.out.println("skip invalid rowkey: " + e.getMessage());
              continue;
            }
            else
            {
              throw e;
            }
          }
          // System.out.println("Added key:"
          // + t.toString().replace(delimiter, " "));
          samples.add(t);
          key = reader.createKey();
          ++records;
          if ((i + 1) * samplesPerSplit <= records) {
            break;
          }
        }
        reader.close();
      }
      return samples.toArray(new Text[samples.size()]);
    }
  }

  public static Text getDispatchKey(Text value, String delimiter, RowKeyDesc rowKeyDesc) {
    String[] vec = value.toString().split(delimiter, -1);
    int[] sortColumns = rowKeyDesc.getOrgColumnIndex();
    if (!rowKeyDesc.isRowkeyValid(vec)) {
      StringBuilder err_sb = new StringBuilder("invalid rowkey, skip this line, ");
      for (int i = 0; i < sortColumns.length; i++) {
        err_sb.append("[index:" + i + ", type:" + rowKeyDesc.getColumnType()[i] + ", val:" + vec[sortColumns[i]]
            + "], ");
      }
      throw new IllegalArgumentException(err_sb.toString());
    }
    StringBuilder sb = new StringBuilder(vec[sortColumns[0]]);
    for (int i = 1; i < sortColumns.length; i++) {
      sb.append(delimiter);
      sb.append(vec[sortColumns[i]]);
    }
    return new Text(sb.toString());
  }

  protected static class LineRecordWriter<K, V> implements RecordWriter<K, V> {
    private static final String utf8 = "UTF-8";
    private static final byte[] newline;
    static {
      try {
        newline = "\n".getBytes(utf8);
      } catch (UnsupportedEncodingException uee) {
        throw new IllegalArgumentException("can't find " + utf8 + " encoding");
      }
    }

    protected DataOutputStream out;
    private final byte[] keyValueSeparator;

    public LineRecordWriter(DataOutputStream out, String keyValueSeparator) {
      this.out = out;
      try {
        this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
      } catch (UnsupportedEncodingException uee) {
        throw new IllegalArgumentException("can't find " + utf8 + " encoding");
      }
    }

    public LineRecordWriter(DataOutputStream out) {
      this(out, "\t");
    }

    /**
     * Write the object to the byte stream, handling Text as a special case.
     *
     * @param o
     *          the object to print
     * @throws IOException
     *           if the write throws, we pass it on
     */
    private void writeObject(Object o) throws IOException {
      if (o instanceof Text) {
        Text to = (Text) o;
        out.write(to.getBytes(), 0, to.getLength());
      } else {
        out.write(o.toString().getBytes(utf8));
      }
    }

    public synchronized void write(K key, V value) throws IOException {

      boolean nullKey = key == null || key instanceof NullWritable;
      boolean nullValue = value == null || value instanceof NullWritable;
      if (nullKey && nullValue) {
        return;
      }
      if (!nullKey) {
        writeObject(key);
      }
      if (!(nullKey || nullValue)) {
        out.write(keyValueSeparator);
      }
      if (!nullValue) {
        writeObject(value);
      }
      out.write(newline);
    }

    public synchronized void close(Reporter reporter) throws IOException {
      out.close();
    }
  }

  /**
   * Write a partition file for the given job, using the Sampler provided.
   * Queries the sampler for a sample keyset, sorts by the output key
   * comparator, selects the keys for each rank, and writes to the destination
   * returned from
   * {@link org.apache.hadoop.mapred.lib.TotalOrderPartitioner#getPartitionFile}
   * .
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  // getInputFormat, getOutputKeyComparator
  public static void writePartitionFile(JobConf job, Sampler sampler) throws IOException {
    final InputFormat inf = job.getInputFormat();
    if (!(inf instanceof TextInputFormat || inf instanceof SequenceFileAsTextInputFormat)) {
      throw new IOException("InputFormat " + inf.getClass().getName() + " not supported!");
    }
    int numPartitions = job.getNumReduceTasks();
    if (numPartitions <= 1) {
      throw new IOException("reduce task num must larger than 1");
    }

    Text[] samples = sampler.getSample(inf, job);
    RawComparator<Text> comparator = (RawComparator<Text>) job.getOutputKeyComparator();
    Arrays.sort(samples, comparator);
    Path dst = new Path(TotalOrderPartitioner.getPartitionFile(job));
    FileSystem fs = dst.getFileSystem(job);
    if (fs.exists(dst)) {
      fs.delete(dst, false);
    }
    String keyValueSeparator = job.get("mapred.textoutputformat.separator", "\t");
    FSDataOutputStream fileOut = fs.create(dst);
    LineRecordWriter<Text, NullWritable> writer = new LineRecordWriter<Text, NullWritable>(fileOut, keyValueSeparator);
    NullWritable nullValue = NullWritable.get();
    float stepSize = samples.length / (float) numPartitions;
    int last = 0;
    if (stepSize >= 1) {
      for (int i = 1; i < numPartitions; ++i) {
        int k = Math.round(stepSize * i);
        while (k < samples.length && comparator.compare(samples[last], samples[k]) >= 0) {
          System.err.printf("sample[" + samples[last].toString() + "] and [" + samples[k].toString()
              + "] are not in sequence!\n");
          ++k;
        }
        if (k == samples.length) {
          System.err.println("=========================");
          for (Text sample : samples) {
            System.err.println(sample);
          }
          System.err.println("=========================");
          throw new ArrayIndexOutOfBoundsException("step[" + k + "] should not exceed " + "samples's length["
              + samples.length + "], stepSize[" + stepSize + "], i[" + i + "]");
        }
        writer.write(samples[k], nullValue);
        last = k;
      }
    } else {
      for (int i = 0; i < samples.length; ++i) {
        writer.write(samples[i], nullValue);
      }
    }
    writer.close(null);
  }
}
