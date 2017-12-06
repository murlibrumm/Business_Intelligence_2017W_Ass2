package at.ac.tuwien.bi_ass2_53.bi_ass2_mapReduce_53;

// Basic MapReduce utility classes
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
// Classes for File IO
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
// Wrappers for data types
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
// Configurable counters
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class MrManager extends Configured implements Tool {
	
	final static Logger logger = Logger.getLogger(MrManager.class); 

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new MrManager(), args);
		System.exit(res);
	}

	// The run method configures and starts the MapReduce job.

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "mrmanager");
		boolean runMR = true;
		for (int i = 0; i < args.length; i += 1) {
			if ("-skipMR".equals(args[i])) {
				runMR = false;
			}
			if ("-no_case".equals(args[i])) {
				job.getConfiguration().setBoolean("mrmanager.case.sensitive", true);
			}
			if ("-pos".equals(args[i])) {
				job.getConfiguration().setBoolean("mrmanager.pos.patterns", true);
				i += 1;
				job.addCacheFile(new Path(args[i]).toUri());
			}    
			if ("-neg".equals(args[i])) {
				job.getConfiguration().setBoolean("mrmanager.neg.patterns", true);
				i += 1;
				job.addCacheFile(new Path(args[i]).toUri());
			}
		}

		int result = 0;
		if (runMR) {
			// Standard job methods
			job.setJarByClass(this.getClass());
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.setMapperClass(at.ac.tuwien.bi_ass2_53.bi_ass2_mapReduce_53.Map.class);
			job.setCombinerClass(at.ac.tuwien.bi_ass2_53.bi_ass2_mapReduce_53.Reduce.class);
			job.setReducerClass(at.ac.tuwien.bi_ass2_53.bi_ass2_mapReduce_53.Reduce.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			result = job.waitForCompletion(true) ? 0 : 1;
		}
		
		/* 
		 * 
		 * Work with the results before returning control to the main method.
		 * 
		 */ 

		HashMap<String, Integer[]> asinMap = collectStrings(new Path(args[1]));
		for (Entry<String, Integer[]> entry : asinMap.entrySet()) { 
			 printResults(entry);
		}
		
		/* 
		 * 
		 * Return and finish.
		 * 
		 */

		return result;
	}
	
	private void printResults(Entry<String, Integer[]> entry) {
		String asin = entry.getKey();
		float good = entry.getValue()[0];
		float bad = entry.getValue()[1];

		if (good + bad > 0) {
			// Calculate the basic sentiment score by dividing the difference 
			// of good and bad words by their sum.

			float sentiment = ((good - bad) / (good + bad));

			// Calculate the positivity score by dividing good results by the sum of
			// good and bad results. Multiply by 100 and round off to get a percentage.
			// Results 50% and above are more positive, overall.		

			float positivity = (good / (good + bad))*100;
			int positivityScore = Math.round(positivity);

			// Display the results in the console.

			System.out.println("**********");
			System.out.println("Results for item: " + asin);
			//System.out.println("Sentiment score = (" + good + " - " + bad + ") / (" + good + " + " + bad + ")");
			System.out.println("Sentiment score = " + sentiment);
			//System.out.println("Positivity score = " + good + "/(" + good + "+" + bad + ")");
			System.out.println("Positivity score = " + positivityScore + "%");
			System.out.println("**********\n");
		}
		else {
			System.out.println("**********");
			System.out.println("No positive or negative words found for item: " + asin);
			System.out.println("**********\n");
		}
	}
	
	// from: https://github.com/rathboma/hadoop-testhelper/blob/master/src/main/java/com/matthewrathbone/hadoop/MRTester.java
	// returns HashMap with String asin, Integer[] where [0] is pos, [1] is neg
	public HashMap<String, Integer[]> collectStrings(Path location) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://quickstart.cloudera:8020");
		FileSystem fileSystem = FileSystem.get(conf);

		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		FileStatus[] items = fileSystem.listStatus(location);
		if (items == null) {
			return new HashMap<String, Integer[]>();
		}
		HashMap<String, Integer[]> asinCounts = new HashMap<String, Integer[]>();
		for (FileStatus item : items) {
			if (item.getPath().getName().startsWith("_")) {
				continue;
			}

			CompressionCodec codec = factory.getCodec(item.getPath());
			InputStream stream = null;

			// check if we have a compression codec
			if (codec != null) {
				stream = codec.createInputStream(fileSystem.open(item.getPath()));
			} else {
				stream = fileSystem.open(item.getPath());
			}

			StringWriter writer = new StringWriter();
			IOUtils.copy(stream, writer, "UTF-8");
			String raw = writer.toString();
			String[] resulting = raw.split("\n");
			for (String line : raw.split("\n")) {
				String[] splitLine = line.split("\t");
				if (splitLine.length == 2) {
					char type = splitLine[0].charAt(0);
					String asin = splitLine[0].substring(1, splitLine[0].length());
					if(!asinCounts.containsKey(asin)){
						asinCounts.put(asin, new Integer[]{0, 0});
					}
					Integer[] contents = asinCounts.get(asin);
					if (type == 'P') {
						contents[0] += Integer.parseInt(splitLine[1].trim());
						asinCounts.put(asin, contents);
					}
					if (type == 'N') {
						contents[1] += Integer.parseInt(splitLine[1].trim());
						asinCounts.put(asin, contents);
					}
				}
			}
		}
		return asinCounts;
	}
	
	
}