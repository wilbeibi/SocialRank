package edu.stevens.cs549.hadoop.socialrank;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class ReciprocityRed2 extends Reducer<Text, Text, Text, Text> {

	/* TODO: Your reducer code here */
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		double bidirectional = 0; // counter variable
		double links = 0;
		for (Text v : values) // Iterates over the values
		{
			int num = Integer.parseInt(v.toString()); // Converts the value to a number
			if (num == 2) // If bidirectional, increments counter
			{
				bidirectional++;
			}
			links += num; // Summing number of links
		}

		double ratio = bidirectional / links; // Calculates ratio
		context.write(new Text(ratio + ""), new Text()); // Outputs: ratio
	}
}
