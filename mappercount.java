package com.hadoopexpert;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class mappercount extends Mapper<LongWritable,Text,Text,IntWritable>{
	public void map(LongWritable key,Text value,Context context)
	{
				String s =value.toString();
				for(String words : s.split(" "))
				{
					if(words.length()>0)
					{
						try
						{
						context.write(new Text(words), new IntWritable(1));
						}
						catch(Exception e)
						{
							System.out.println(e);
						}
				}
					
	}
		

}
}
