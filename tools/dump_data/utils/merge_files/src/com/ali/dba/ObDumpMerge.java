package com.ali.dba;

import java.util.*;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ObDumpMerge implements Tool {
	public static final Log LOG = LogFactory.getLog(ObDumpMerge.class);
	private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
	static {
		NUMBER_FORMAT.setGroupingUsed(false);
	}
	private final static String MERGE_TEMP = "_merge_tmp";
	private JobConf conf;

	public ObDumpMerge(JobConf conf) {
		setConf(conf);
	}

	@Override
	public Configuration getConf() {
		return this.conf;
	}

	@Override
	public void setConf(Configuration conf) {
		if (conf instanceof JobConf) {
			this.conf = (JobConf) conf;
		} else {
			this.conf = new JobConf(conf);
		}
	}

	static class MergeFilesMapper implements
			Mapper<LongWritable, Text, WritableComparable<?>, Text> {

		private Path root;
		private Path bak;
		private FileSystem fs;
		private Configuration conf;
    private Path mergedDir;

		@Override
		public void configure(JobConf job) {
			root = new Path(job.get("ytht.root.dir"));
			bak = new Path(job.get("ytht.backup.dir"));
      mergedDir = new Path(job.get("ytht.merged.dir"));
			LOG.info("root dir: " + root);
			LOG.info("backup dir: " + bak);
			
			conf = job;
			try {
				fs = FileSystem.get(job);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<WritableComparable<?>, Text> output,
				Reporter reporter) throws IOException {

			String file_list = value.toString();
      String [] files = file_list.split(",");
			LOG.info("File list for merge: " + files);

      Path []filePath = new Path[files.length];
      for (int i = 0;i < files.length;i++) {
        filePath[i] = new Path(files[i]);
      }

      mergeFiles(root, filePath, fs, mergedDir, reporter, conf);
//			mergeDir(root, new Path(dir), bak, fs, conf, reporter);
		}

		@Override
		public void close() throws IOException {

		}
	}

	private static CompressionType getCompressionType(SequenceFile.Reader in) {
		if(in.isCompressed()) 
			if(in.isBlockCompressed())
				return CompressionType.BLOCK;
			else
				return CompressionType.RECORD;
		else
			return CompressionType.NONE;
	}
	
	@SuppressWarnings("unchecked")
	private static void copyKeyValue(SequenceFile.Reader in, SequenceFile.Writer out,
			Configuration conf) throws IOException {
		WritableComparable key = ReflectionUtils.newInstance(in.getKeyClass().asSubclass(
				WritableComparable.class), conf);
		Writable val = ReflectionUtils.newInstance(in.getValueClass().asSubclass(
				Writable.class), conf);
		try {
			while(in.next(key, val)) {
				out.append(key, val);
			}
		} catch (IOException ee) {
			ee.printStackTrace();
			System.out.println("Got IOException when reading seq file. " +
					"Maybe EOFException, but continue...");
		}
	}
	
	private static void mergeSequenceFile(FileSystem srcFS, Path srcDir,
			FileSystem dstFS, Path dstFile, Configuration conf, Reporter reporter) throws IOException {
		FileStatus contents[] = srcFS.listStatus(srcDir);
		int len = contents.length;
		SequenceFile.Reader in = null;
		for (int i = 0; i < contents.length; i++) {
			if (!contents[i].isDir()) {
				in = new SequenceFile.Reader(
						srcFS, contents[i].getPath(), conf);
				break;
			}
		}
		
		if (in == null)
			return;
		
		SequenceFile.Writer out = SequenceFile.createWriter(
				dstFS, conf, dstFile, in.getKeyClass(), in.getValueClass(),
				getCompressionType(in), in.getCompressionCodec());
		
		try {
			for (int i = 0; i < contents.length; i++) {
				if (!contents[i].isDir() && contents[i].getLen() > 0) {
					in = new SequenceFile.Reader(
							srcFS, contents[i].getPath(), conf);
					try {
						reporter.setStatus("merging dir: " 
								+ srcDir + " (" + (i+1) + "/" + len + ")");
						copyKeyValue(in, out, conf);
					} finally {
						in.close();
					}
				}
			}
		} finally {
			out.close();
		}
	}
	
	private static void mergeCompressedFile(FileSystem srcFS, Path srcDir,
			FileSystem dstFS, Path dstFile, Configuration conf,
			CompressionCodec codec, Reporter reporter) throws IOException {		
		OutputStream out = codec.createOutputStream(dstFS.create(dstFile));
		
		long maxSize = conf.getLong("ytht.compressed.file.maxsize", -1);
		long totalSize = 0L;

		try {
			FileStatus contents[] = srcFS.listStatus(srcDir);
			int totalFiles = contents.length;
			for (int curFile = 0; curFile < contents.length; curFile++) {
				if (!contents[curFile].isDir() && contents[curFile].getLen() > 0) {
					long fileSize = contents[curFile].getLen();
					totalSize += fileSize;
					if (maxSize != -1 && totalSize > maxSize && fileSize != totalSize) {
						// generate new file
						out.close();
						
						totalSize = fileSize;
						dstFile = getNextFileName(dstFile);
						out = codec.createOutputStream(dstFS.create(dstFile));
					}
				
					InputStream in = codec.createInputStream(srcFS
							.open(contents[curFile].getPath()));
					try {
						reporter.setStatus("merging dir: " 
								+ srcDir + " (" + (curFile+1) + "/" + totalFiles + ")");
						IOUtils.copyBytes(in, out, conf, false);
					} finally {
						in.close();
					}
				}
			}
		} finally {
			out.close();
		}
	}
	
	static Path getNextFileName(Path dstFile) {
		String prev = dstFile.getName();
		String next = null;
		
		Pattern p1 = Pattern.compile("part-(\\d{5,6})" +  // part-xxx
				"(\\.(deflate|gz|bz2))?"); // suffix
		Pattern p2 = Pattern.compile(
				"attempt_\\d{12}_\\d{4,7}_[rm]_(\\d{6})_\\d" + // attempt_xxx
				"(\\.(deflate|gz|bz2))?"); // suffix
		Matcher m1 = p1.matcher(prev);
		Matcher m2 = p2.matcher(prev);
		if (m1.find()) {
			String num = m1.group(1);
			int n = Integer.parseInt(num) + 1;
			NUMBER_FORMAT.setMinimumIntegerDigits(num.length());
			next = prev.replace("part-" + num, "part-" + NUMBER_FORMAT.format(n));
		} else if (m2.find()) {
			String num = m2.group(1);
			int n = Integer.parseInt(num) + 1;
			NUMBER_FORMAT.setMinimumIntegerDigits(num.length());
			next = prev.replace("_" + num, "_" + NUMBER_FORMAT.format(n));
		}
		return new Path(dstFile.getParent(), next);
	}

	private static void mergeNormalFile(FileSystem srcFS, Path srcDir,
			FileSystem dstFS, Path dstFile, Configuration conf, Reporter reporter)
			throws IOException {
		OutputStream out = dstFS.create(dstFile);

		try {
			FileStatus contents[] = srcFS.listStatus(srcDir);
			int len = contents.length;
			for (int i = 0; i < contents.length; i++) {
				if (!contents[i].isDir()) {
					InputStream in = srcFS.open(contents[i].getPath());
          System.out.println(contents[i].getPath().toString());
					try {
						reporter.setStatus("merging dir: " 
								+ srcDir + " (" + (i+1) + "/" + len + ")");
						IOUtils.copyBytes(in, out, conf, false);
					} finally {
						in.close();
					}
				}
			}
		} finally {
			out.close();
		}
	}

  private static void mergeFiles(Path root, Path [] files, FileSystem fs, Path backupDir, 
      Reporter reporter, Configuration conf) throws IOException {

    if (files.length < 1) {
      System.out.println("No file te be proceed");
      return;
    }

    System.out.println("Gernerating output file:" + backupDir.toString());

    String relative = files[0].toString().substring(
        root.toString().length() + 1);

		OutputStream out = fs.create(new Path(backupDir, relative));

    try {
      FileStatus contents[] = fs.listStatus(files);
      int len = contents.length;

      for (int i = 0; i < contents.length; i++) {
        if (!contents[i].isDir()) {
          InputStream in = fs.open(contents[i].getPath());
          System.out.println("Merging input file:" + files[i]);

          try {
            reporter.setStatus("merging dir: " 
                + root + " (" + (i+1) + "/" + len + ")");

            IOUtils.copyBytes(in, out, conf, false);
          } finally {
            in.close();
          }
        }
      }
    } finally {
      out.close();
    }
  }

	/*
	 * dir can contain files like: 
	 * _logs, 
	 * part-00000 
	 * part-00001 
	 * ... 
	 * part-000xx
	 * 
	 * parameter speedUpFiles is used to reduce calls of listStatus
	 */
	private static boolean needMerge(Path dir, FileSystem fs, Configuration conf,
			List<FileStatus> speedUpFiles, List<Path> oneFile) throws IOException {
		if (speedUpFiles != null) {
			speedUpFiles.clear();
		}

		// file pattern 
		//  part-00000
		//  part-00000.deflate
		//  attempt_201003291206_202753_r_000159_0
		//  attempt_201003291206_311567_r_000128_0.deflate
		Pattern p = Pattern.compile("((part-\\d{5,6})" + "|" +  // part-xxx
				"(attempt_\\d{12}_\\d{4,7}_[rm]_\\d{6}_\\d))" + // attempt_xxx
				"(\\.(deflate|gz|bz2))?"); // suffix
		long avgSize = conf.getLong("ytht.average.filesize", 1024 * 1024 * 1024);

		FileStatus[] files = fs.listStatus(dir);
		if(files == null) { 
			// dir deleted
			return false;
		}
		long totalSize = 0;
		int num = 0;
		String firstFile = null;
		for (FileStatus f : files) {
			String fileName = f.getPath().getName();
			if (fileName.equals("_logs")) {
				continue;
			}
			if (!p.matcher(fileName).matches()) {
				// if not all files are part-000xx
				// use speed up file list
				if (speedUpFiles != null) {
					speedUpFiles.addAll(Arrays.asList(files));
				}
				return false;
			} else {
				if (firstFile == null) {
					firstFile = fileName;  
				}
				num++;
				totalSize += f.getLen();
			}
		}
    // files too large, no need to merge
		if (num < 2 || (totalSize / num > avgSize)) {
			LOG.info("Files too large, no need to merge, num=" + num + ",avgSize=" + avgSize + ", per_size=" + totalSize / num);
			return false;
    }

		if(conf.get("ytht.dir.pattern") != null && 
				!conf.get("ytht.dir.pattern").equals("")) {
			Pattern dir_p = Pattern.compile(conf.get("ytht.dir.pattern"));
			if (!dir_p.matcher(dir.getName()).matches()) // dir pattern not matches 
				return false;
		}
		
		LOG.info("dir " + dir.toString() + " (" + num + ") need merge");
		// trick: set a file such as part-00000
		if (oneFile != null) {
			oneFile.clear();
			oneFile.add(new Path(dir, firstFile));
		}
		return true;
	}

  private void generateFileListForMerge(Path dir, FileSystem fs, JobConf conf,
      List<Path> toMerge) throws IOException {
    FileStatus[] files = fs.listStatus(dir);

    if(files == null) { 
      LOG.info("No such dir:" + dir.getName());
      return;
    }

    for (FileStatus f : files) {
      Path p = new Path(dir, f.getPath().getName());
      toMerge.add(p);
      LOG.info("Adding file to merge" + p.getName());
    }

    //2011-11-08
    Collections.shuffle(toMerge, new Random()); 
  }

	private void generateDirListForMerge(Path dir, FileSystem fs, JobConf conf,
			List<Path> toMerge) throws IOException {
		List<FileStatus> speedup = new LinkedList<FileStatus>();
		List<Path> oneFile = new ArrayList<Path>(1); 

		if (needMerge(dir, fs, conf, speedup, oneFile)) {
			toMerge.add(oneFile.get(0));
		} else {
			if (!speedup.isEmpty()) {
				for (FileStatus f : speedup) {
					if (f.isDir()) {
						generateDirListForMerge(f.getPath(), fs, conf, toMerge);
					}
				}
			}
		}
	}

	private void setup(JobConf conf, String targetPath, String backupPath)
			throws IOException {
		conf.setJobName("ObDumpMerge");

		FileSystem fs = FileSystem.get(conf);
		Path root = new Path(targetPath);
		root = root.makeQualified(fs);
		Path bak = new Path(backupPath);
		bak = bak.makeQualified(fs);
		
		if (!fs.getFileStatus(root).isDir()) {
			throw new IOException("Target path for ObDumpMerge is not a dir.");
		}
		if (fs.exists(bak)) {
			if (!fs.getFileStatus(bak).isDir()) {
				throw new IOException("Backup path for ObDumpMerge is not a dir.");
			}
		}

//		fs.mkdirs(new Path(bak, MERGE_TEMP)); 
//    Path mergedDir = new Path(bak, MERGE_TEMP);

    Path mergedDir = bak;

		List<Path> toMerge = new LinkedList<Path>();
    generateFileListForMerge(root, fs, conf, toMerge);

		int fileNum = toMerge.size();
    LOG.info("Needed to be mereged fileNum:" + fileNum);

		if (fileNum == 0) {
			LOG.info("No file need to be merged. Exiting...");
    } else {
      // set up a job
      String randomId = Integer.toString(new Random()
          .nextInt(Integer.MAX_VALUE), 36);

      Path jobDir = new Path(bak, "_ytht_files_" + randomId);
      Path input = new Path(jobDir, "to_merge");
      int num = conf.getNumMapTasks();
      int avg = fileNum / num;
      int fileId = 0;

      fs.mkdirs(jobDir); 
      for (int i = 0; i < fileNum; i += avg){
        PrintWriter out = new PrintWriter(fs.create(new Path(input,
                "input-" + fileId++)));
        LOG.info("Writing Map task files, jobDir=" + jobDir.getName());

        String merge_files = new String();
        for( int j = i; j < (avg + i) && j < fileNum; j++){
          if (i != j)
            merge_files += ",";

          merge_files += toMerge.get(j).toString();
        }

        out.println(merge_files);
        out.close();
      }

      Path output = new Path(jobDir, "result");
      FileInputFormat.setInputPaths(conf, input);
      FileOutputFormat.setOutputPath(conf, output);
      conf.set("ytht.root.dir", root.toString());
      conf.set("ytht.backup.dir", bak.toString());
      conf.set("ytht.job.dir", jobDir.toString());
      conf.set("ytht.merged.dir", mergedDir.toString());
      conf.setMapSpeculativeExecution(false);
      conf.setNumReduceTasks(0);
      conf.setMapperClass(MergeFilesMapper.class);
      conf.setInt("mapred.task.timeout", 0);
    }
	}

	private void cleanup(JobConf conf, String backupPath) throws IOException {
		FileSystem fs = FileSystem.get(conf);

		String jobDir = conf.get("ytht.job.dir");
		if(jobDir != null) {
      System.out.println("cleanup:JobDir=" + jobDir);
			fs.delete(new Path(jobDir), true);
		}
	}
	
	private List<String> parseArgument(String[] args) {
		List<String> other_args = new ArrayList<String>();		
		for (int idx = 0; idx < args.length; idx++) {
			if ("-m".equals(args[idx])) {
				if (++idx ==  args.length) {
		            throw new IllegalArgumentException("map number not specified in -m");
		        }
				try {
					int maps = Integer.parseInt(args[idx]);
					if (maps <= 0) {
						throw new IllegalArgumentException("map number must be larger than 0");
					}
					conf.setNumMapTasks(maps);
				} catch (NumberFormatException e) {
					throw new IllegalArgumentException("Integer expected but got " + args[idx]);
				}
			} else if ("-p".equals(args[idx])) {
				if (++idx ==  args.length) {
		            throw new IllegalArgumentException(
		            		"dir pattern for merge not specified in -p");
		        }
				conf.set("ytht.dir.pattern", args[idx]);
			} else {
				other_args.add(args[idx]);
			}
		}
		if (other_args.size() < 2) {
			throw new IllegalArgumentException("not enough arguments");
		}
		return other_args;
	}

	@Override
	public int run(String[] args) throws Exception {
		try {
			List<String> other_args = parseArgument(args);

			if (conf.getNumMapTasks() > 2) {
				LOG.info("Max running threads: " + conf.getNumMapTasks());
			}

			// TODO: if no backup dir is set, using /path/to/merge/../_ytht_backup
			// TODO: parameter settings
			setup(conf, other_args.get(0), other_args.get(1));
			if (conf.get("ytht.job.dir") != null) {
				JobClient.runJob(conf);
			}
			cleanup(conf, args[1]);
		} catch (IllegalArgumentException e) {
			printUsage();
			return -1;
		} catch (IOException e) {
			LOG.error("ObDumpMerge: get exception when merging files");
			e.printStackTrace();
			return -1;
		}

		return 0;
	}

	private void printUsage() {
		System.out.println("Usage: ObDumpMerge [ -p dir_pattern ] [ -m map_num ] dir backup_dir ");
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(ObDumpMerge.class);
		int res = ToolRunner.run(new ObDumpMerge(conf), args);
		System.exit(res);
	}

}
