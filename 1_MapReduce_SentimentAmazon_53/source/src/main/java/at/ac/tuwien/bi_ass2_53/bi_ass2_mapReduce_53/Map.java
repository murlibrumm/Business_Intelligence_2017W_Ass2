package at.ac.tuwien.bi_ass2_53.bi_ass2_mapReduce_53;

// Basic Java file IO 
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
// Regular expression utility
import java.util.regex.Pattern;

// Mapper parent class and the Configuration class
import org.apache.hadoop.conf.Configuration;
// Wrappers for values
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.StringUtils;
import org.json.JSONObject;


public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

	// Enumeration used for custom counters
	enum Gauge{POSITIVE, NEGATIVE}

	// IntWritable object set to the value 1 as counting increment.
	private final static IntWritable one = new IntWritable(1);

	// Reusable variable for each word instance in the incoming data.
	private Text word = new Text();

	// Store case sensitivity setting from command line.
	private boolean caseSensitive = false;

	private String input;

	// HashSets for filter terms.
	private Set<String> goodWords = new HashSet<String>();
	private Set<String> badWords = new HashSet<String>();

	// Word boundary defined as whitespace-characters-word boundary-whitespace 
	private static final 
	Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException
	{
		// If the input for this mapper is a file reference, read from the
		// referenced file. Otherwise, read from the InputSplit itself.
		if (context.getInputSplit() instanceof FileSplit)
		{
			this.input = ((FileSplit) context.getInputSplit()).getPath().toString();
		} else {
			this.input = context.getInputSplit().toString();
		}

		// Check for and set boolean runtime variables. 
		Configuration config = context.getConfiguration();

		this.caseSensitive = config.getBoolean("mrmanager.case.sensitive", false);

		URI[] localPaths = context.getCacheFiles();

		parseFile(localPaths[0], "positive");
		parseFile(localPaths[1], "negative");
	}

	// Parse the positive words to match and capture during Map phase.
	private void parseFile(URI fileUri, String type) {
		try {
			BufferedReader fis = new BufferedReader(new FileReader(new File(fileUri.getPath()).getName()));
			String word;
			while ((word = fis.readLine()) != null) {
				if (type.equals("positive")) {
					goodWords.add(word);
				}
				if (type.equals("negative")) {
					badWords.add(word);
				}
			}
		} catch (IOException ioe) {
			System.err.println("Caught exception parsing cached file '" + type + "' : " + StringUtils.stringifyException(ioe));
		}
	}

	public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
		
		String line = lineText.toString();
		JSONObject obj = new JSONObject(line);
		String reviewText = obj.getString("reviewText");
		String asin = obj.getString("asin");
		
		// If caseSensitive is false, convert everything to lower case.
		if (!caseSensitive) {
			reviewText = reviewText.toLowerCase();
		}

		// Store each the current word in the queue for processing.
		for (String word : WORD_BOUNDARY.split(reviewText))
		{
			if (word.isEmpty()) {
				continue;
			}

			// Filter and count "good" words.
			if (goodWords.contains(word)) {
				context.write(new Text("P" + asin), one);
			}

			// Filter and count "bad" words.
			if (badWords.contains(word)) {
				context.write(new Text("N" + asin), one);
			}
		}
	}
}