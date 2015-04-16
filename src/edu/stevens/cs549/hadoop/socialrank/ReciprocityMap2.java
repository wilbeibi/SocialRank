package edu.stevens.cs549.hadoop.socialrank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class ReciprocityMap2 extends Mapper<LongWritable, Text, Text, Text> {

	/* TODO: Your mapper code here */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {

		int num = 0;
		if (!(((key.toString()).split("\t")).length > 1))
			num = Integer.parseInt((value.toString()).trim()); // Converts Line to a String
		else
			throw new IOException("Incorrect Input Data format");

		context.write(new Text("Links"), new Text(num + "")); // Outputs: Key: "Links" Value: number of links

	}
}
