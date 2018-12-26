package com;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class WordCount {
	
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		private final static LongWritable lw = new LongWritable();
		private Text word = new Text();
		
		@Override		// 에러나면 제대로 안된것
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer st = new StringTokenizer(line, "\t\r\n\f|,.()<>"); // 안에있는 값들 다 잘라버린다
			while(st.hasMoreTokens()) {
				word.set(st.nextToken().toLowerCase());		// 대소문자 구문 없이 소문자로만, 마지막에 없으면 펄스
				context.write(word,lw);
			}
		}
	}
	
	public static class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
		
		private LongWritable lw = new LongWritable();
		
		protected void recude(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			for(LongWritable value:values) {
				sum += value.get();
			}
			lw.set(sum);
			context.write(key, lw);
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"WordCount");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path("wordcount.txt"));
		FileOutputFormat.setOutputPath(job, new Path("wordcount.log"));
		job.waitForCompletion(true);
		
	}
}
