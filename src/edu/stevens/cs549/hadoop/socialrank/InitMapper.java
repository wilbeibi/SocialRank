package edu.stevens.cs549.hadoop.socialrank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class InitMapper extends Mapper<LongWritable, Text, Text, Text> {

	/* TODO: Your mapper code here */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String

		if (line.contains("\t")) {
			String[] parts = line.split("\t"); // Splits the line into the 2 vertices
			if (parts[0].matches("[0-9]+") && parts[1].matches("[0-9]+")) { // Makes sure the vertices are numbers only
				context.write(new Text(parts[0]), new Text(parts[1])); // Emits each vertex with 1 of it's friends
			} else {
				throw new IllegalArgumentException("Input data format incorrect"); // Error in case of incorrect format
			}
		} else {
			throw new IllegalArgumentException("Input data format incorrect"); // Error in case of incorrect format
		}

	}

}
