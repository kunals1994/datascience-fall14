package org.myorg;

	import java.io.IOException;
	import java.util.*;
	import java.util.regex.*;
	
	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.conf.*;
	import org.apache.hadoop.io.*;
	import org.apache.hadoop.mapred.*;
	import org.apache.hadoop.util.*;
	
	public class BigramCount {
	
		public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
			private final static IntWritable one = new IntWritable(1);
			private Text curr = new Text();
			private Text prev = new Text();

			public void map(LongWritable key, Text value, OutputCollector output, Reporter reporter) throws IOException {
				String line = value.toString();
				StringTokenizer tokenizer = new StringTokenizer(line);
				while (tokenizer.hasMoreTokens()) {
					//First iteration, prev is not set. 
					if (prev == null){
						curr.set(tokenizer.nextToken());
						continue;
					}

					prev.set(curr);
					curr.set(tokenizer.nextToken());
					output.collect(new Text(prev.toString() + " " + curr.toString()), one);
				}
			}
		}
	
		public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
			public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
				int sum = 0;
				while (values.hasNext()) {
					sum += values.next().get();
				}
				output.collect(key, new IntWritable(sum));
			}
		}


		public static class MapTwo extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
			private Text out = new Text();

			public void map(LongWritable key, Text value, OutputCollector output, Reporter reporter) throws IOException {
				String line = value.toString();
				int spaceOne = line.indexOf(" ");
				int spaceTwo = line.indexOf("\t");

				String one = line.substring(0, spaceOne);
				String two = line.substring(spaceOne + 1, spaceTwo);

				out.set(line);

				output.collect(new Text(one), out);
				output.collect(new Text(two), out);
			}
		}
	
		public static class ReduceTwo extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
			public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
				String [] topFive = new String[5];
				int [] topFiveValues = new int [5];

				while (values.hasNext()) {
					String curr = values.next().toString();


					//For some reason Text objects were being converted to some weird format on my hadoop install; Parsing to a proper string below. 
					Pattern pat = Pattern.compile("[a-z]+ [a-z]+\t[0-9]+");
					Matcher mat = pat.matcher(curr);

					if(mat.find()){
						curr = mat.group(0);
					}else{
						continue;
					}

					int currVal = 0;
					try{
						currVal = Integer.parseInt(curr.substring(curr.indexOf("\t") + 1));
					}catch (Exception e){
						currVal = -1;
					}

					int index = -1;
					while(index < 4 && currVal >= topFiveValues[index + 1]){
						index += 1;
					}

					if(index == -1){
						continue;
					}

					int temp = index; 
					while(temp > 0){
						topFive[temp - 1] = topFive[temp];
						topFiveValues[temp - 1] = topFiveValues[temp];

						temp -=1;
					}

					topFive[index] = curr;
					topFiveValues[index] = currVal;

				}

				String ret = new String("[");
				for (String s : topFive){
					ret += s + ",\t";
				}

				ret += "]";

				output.collect(key, new Text(ret));
			}
		}



		public static void jobOne(String [] args) throws Exception{
			JobConf conf = new JobConf(BigramCount.class);
			conf.setJobName("bigramcount-part1");
	
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

		public static void jobTwo(String [] args) throws Exception{
			JobConf conf = new JobConf(BigramCount.class);
			conf.setJobName("bigramcount-part2");
	
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
	
			conf.setMapperClass(MapTwo.class);
			conf.setCombinerClass(ReduceTwo.class);
			conf.setReducerClass(ReduceTwo.class);
	
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);
	
			FileInputFormat.setInputPaths(conf, new Path(args[0]));
			FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	
			JobClient.runJob(conf);
		}
	
		public static void main(String[] args) throws Exception {
			if(args.length < 3){
				System.err.println("Include job number and file input and output");
				return;
			}

			String [] jobArgs = new String[2];
			jobArgs[0] = args[1];
			jobArgs[1] = args[2];



			if (args[0].equals("1")){
				jobOne(jobArgs);
			}else if(args[0].equals("2")){
				jobTwo(jobArgs);
			}

		}
	}
	
