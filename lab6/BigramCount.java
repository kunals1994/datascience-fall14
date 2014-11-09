package org.myorg;

	import java.io.IOException;
	import java.util.*;
	
	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.conf.*;
	import org.apache.hadoop.io.*;
	import org.apache.hadoop.mapred.*;
	import org.apache.hadoop.util.*;
	
	public class BigramCount {
	
        public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
            private final static IntWritable one = new IntWritable(1);
            private Text curr = new Text();
            private Text prev = null;

            public void map(LongWritable key, Text value, OutputCollector output, Reporter reporter) throws IOException {
                String line = value.toString();
                StringTokenizer tokenizer = new StringTokenizer(line);
                while (tokenizer.hasMoreTokens()) {
                	//First iteration, prev is not set. 
                	if (prev == null){
                		prev = curr; 
                		curr.set(tokenizer.nextToken());
                		continue;
                	}

                	prev = curr;
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

       	public static void jobOne(){
       		JobConf conf = new JobConf(WordCount.class);
	     	conf.setJobName("wordcount");
	
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

       	public static void jobTwo(){

       	}
	
	   	public static void main(String[] args) throws Exception {
	    	if(args.length < 1){
	    		System.out.println("Include job number");
	    	}
	    	else if (args[0].equals("1")){
	    		jobOne();
	    	}else if(args[0].equals("2")){
	    		jobTwo();
	    	}

	   	}
	}
	
