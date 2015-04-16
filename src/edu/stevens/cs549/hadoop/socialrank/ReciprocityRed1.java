package edu.stevens.cs549.hadoop.socialrank;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class ReciprocityRed1 extends Reducer<Text, Text, Text, Text> {

	/* TODO: Your reducer code here */
	@SuppressWarnings("unused")
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		int i = 0; // counter variable
		for (Text v : values) // Iterates over the values
		{
			i++; // Increments counter
		}
		context.write(new Text(i + ""), new Text()); // Outputs:Number of linkes => i (2 if bidirectional, 1 if
														// unidirectional)
	}
}
