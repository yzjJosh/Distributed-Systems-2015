import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class TextAnalyzer {
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, MapWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, MapWritable> output, Reporter reporter)
				throws IOException {
			//Convert the capital chars to lower chars.		
			String line = value.toString().toLowerCase();
			String[] words = line.split("([^a-z0-9])+");
			String[] contextWords = words.clone(); // Set up the context word array.
			String[] queryWords = words.clone(); // Set up the query word array.
			MapWritable map = new MapWritable();
			for (int i = 0; i < contextWords.length; i++) {
				String contextWord = contextWords[i];
            	boolean isSelf = true;  //In order to prevent it from calculating contextWord itself.
            	for (int j = 0; j < queryWords.length; j++) {
            		String queryWord = queryWords[j];
            		if (queryWord.equals(contextWord)) {
            			if (isSelf) {
            				isSelf = false;
            			    continue;
            			}
            			map.put(new Text(queryWord), one);
            			output.collect(new Text(contextWord), map);	
            		} 
            		
            	}
			}

		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, MapWritable, Text, MapWritable> {
		public void reduce(Text key, Iterator<Context> values,
				OutputCollector<Text, MapWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			MapWritable map = new MapWritable();
			while (values.hasNext()) {
				MapWritable map_branch = values.next().get();
				for (Text key : map_branch.keySet()) {    //Iterate the keyset
					if (!map.containsKey(key)) {       //Outcome map doesn't contain the key 
						map.put(key, map_branch.get(key));  
					} else {
						map.put(key, new IntWritable(map.get(key) + 1)); //key appearing num + 1
					}
				}
				
			}
			output.collect(key, map);
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(WordCount.class);
		conf.setJobName("TextAnalyzer");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
	}

}
