package edu.stevens.cs549.hadoop.socialrank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FinReducer extends Reducer<DoubleWritable, Text, Text, Text> {

	/* TODO: Your reducer code here */
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException,
			InterruptedException {
		Double rank = -Double.parseDouble(key.toString());
		for (Text v : values) // iterates over the values
		{
			context.write(v, new Text(rank + "")); // Swaps, negates and writes out the tuples
		}

	}
}
