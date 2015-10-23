import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class TextAnalyzer {
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, MyMap> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, MyMap> output, Reporter reporter)
				throws IOException {
			//Convert the capital chars to lower chars.
			String line = value.toString().toLowerCase();
			line = line.trim();
			String[] words = line.split("([^a-z0-9])+");
			String[] contextWords = words.clone(); // Set up the context word array.
			String[] queryWords = words.clone(); // Set up the query word array.
			Set <String> set = new HashSet<String>();
			for (int i = 0; i < contextWords.length; i++) {
				String contextWord = contextWords[i];
				if(set.contains(contextWord)) {   //The same context word is calculated only once.
					continue;
				}
				set.add(contextWord);
				Text contextText = new Text(contextWord);
            	boolean isSelf = true;  //In order to prevent it from calculating contextWord itself.
            	MyMap map = new MyMap();
            	HashMap <String, Integer> hashMap = new HashMap<String, Integer>();
            	for (int j = 0; j < queryWords.length; j++) {
            		String queryWord = queryWords[j];
            		if (queryWord.equals(contextWord)) {
            			if (isSelf) {
            				isSelf = false;
            			    continue;
            			}
            		}
            		if (!hashMap.containsKey(queryWord)) {       //Outcome map doesn't contain the key 
						hashMap.put(queryWord, 1);
					} else {
						hashMap.put(queryWord, hashMap.get(queryWord) + 1); //key appearing num + 1
					}
            	}
            	for (java.util.Map.Entry<String, Integer> entry: hashMap.entrySet()) {
            		map.put(new Text(entry.getKey()), new IntWritable(entry.getValue()));
            	}
            	output.collect(contextText, map);
			}

		}
	}
	public static class MyMap extends MapWritable {
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("\n");
			for(java.util.Map.Entry<Writable,Writable> entry : this.entrySet()) {
				builder.append("<" + ((Text)entry.getKey()).toString() + ", " + ((IntWritable)entry.getValue()).get() + ">\n");	
			}
			return builder.toString();
		}
	}
	public static class Reduce extends MapReduceBase implements
			Reducer<Text, MyMap, Text, MyMap> {
		public void reduce(Text key, Iterator<MyMap> values,
				OutputCollector<Text, MyMap> output, Reporter reporter)
				throws IOException {
			MyMap map = new MyMap();
			while (values.hasNext()) {
				MyMap map_branch = values.next();
				for (Writable k : map_branch.keySet()) {    //Iterate the keyset
					Text keyOfMap = (Text)k;
					if (!map.containsKey(keyOfMap)) {       //Outcome map doesn't contain the key 
						map.put(keyOfMap, map_branch.get(keyOfMap));
					} else {
						map.put(keyOfMap, new IntWritable(((IntWritable)map.get(keyOfMap)).get() + ((IntWritable)map_branch.get(keyOfMap)).get())); //key appearing num + 1
					}
				}

			}
			output.collect(key, map);
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(TextAnalyzer.class);
		conf.setJobName("TextAnalyzer");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(MyMap.class);
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
