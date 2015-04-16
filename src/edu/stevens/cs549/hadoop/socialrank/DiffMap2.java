package edu.stevens.cs549.hadoop.socialrank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffMap2 extends Mapper<LongWritable, Text, Text, Text> {

	/* TODO: Your mapper code here */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {
		String s = value.toString(); // Converts Line to a String

		context.write(new Text("Difference"), new Text(s)); // emits: key:"Difference" value:difference calculated in
															// DiffRed1

	}

}
