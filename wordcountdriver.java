package com.hadoopexpert;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class wordcountdriver {
	
	@SuppressWarnings("deprecation")
	public static void main(String args[]) throws IOException
	{
		Job job=new Job();
	
		job.setMapperClass(mappercount.class);
		job.setReducerClass(reducercount.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setJarByClass(wordcountdriver.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]) );
		
		try{
		System.exit(job.waitForCompletion(true)?0:1);
		}
		catch(Exception e)
		{
			
		}
		}
		
	}


