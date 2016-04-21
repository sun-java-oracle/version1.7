import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Charcount{
	public static class CharMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			//System.out.println(line);			
			char[] ch = line.toCharArray();
			for(char c : ch)
				context.write(new Text(String.valueOf(c)),new IntWritable(1));
		}
	}
	
	public static class CharReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
		public void reduce(Text key, Iterable<IntWritable> value, Context context)throws IOException,InterruptedException{
			int count = 0;
			IntWritable result = new IntWritable();
			for(IntWritable val : value){
				count+=val.get();				
			}
			result.set(count);
			context.write(key,result);
		}
	}
	
	public Charcount(){}
	public static void main(String args[])throws Exception{
		Configuration conf = new Configuration();
		Job job = new Job(conf,"char count");
		job.setJarByClass(Charcount.class);
		job.setMapperClass(CharMapper.class);
		job.setReducerClass(CharReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job,new Path("/user/shubham/input"));
		FileOutputFormat.setOutputPath(job,new Path("out4"));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}


