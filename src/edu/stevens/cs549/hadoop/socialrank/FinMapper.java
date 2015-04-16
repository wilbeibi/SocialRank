package edu.stevens.cs549.hadoop.socialrank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class FinMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		String[] sections = line.split("\t"); // Splits each line into node;rank friend1,friend2,...,friendn
		if (sections.length > 2) // Checks for incorrect data format
		{
			throw new IOException("Incorrect data format");
		}
		String[] parts = sections[0].split(";");
		Double rank = Double.parseDouble(parts[1]);
		// Outputs negated Key value pairs to exploit Hadoop's key sort function
		context.write(new DoubleWritable(-rank), new Text(parts[0])); // output: key: -rank value: node

	}

}
