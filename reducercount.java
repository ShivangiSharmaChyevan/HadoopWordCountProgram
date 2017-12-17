package com.hadoopexpert;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class reducercount extends Reducer<Text,IntWritable,Text,IntWritable> 
{
	public void reduce(Text key,Iterable<IntWritable> values,Context context){
		
		int total=0;
		while(values.iterator().hasNext())
		{
			IntWritable i=values.iterator().next();
			int i1=i.get();
			
			total+=i1;
			
		}
		try
		{
		context.write(key, new IntWritable(total));
		}
		catch(Exception e)
		{
			System.out.println(e);
		}
	}
}	


