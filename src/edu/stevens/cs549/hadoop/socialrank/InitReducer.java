package edu.stevens.cs549.hadoop.socialrank;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class InitReducer extends Reducer<Text, Text, Text, Text> {

	/* TODO: Your reducer code here */
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String k = key.toString(); // Converts the key to a String
		int rank = 1; // Defaults starting rank == 1

		k = k + ";" + rank; // Makes a composite string of Node and rank in the format : node;rank
		String out = "";

		for (Text value : values) // Iterates over the values
		{
			out = out + value.toString() + ","; // Adds each friend to a string separated by commas
		}

		out = out.substring(0, out.lastIndexOf(',')); // Removes last ,
		context.write(new Text(k), new Text(out)); // Outputs: node;rank friend1,friend2.....,friendn

	}
}
