package edu.stevens.cs549.hadoop.socialrank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class ReciprocityMap1 extends Mapper<LongWritable, Text, Text, Text> {

	/* TODO: Your mapper code here */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String

		if (line.contains("\t")) {
			String[] parts = line.split("\t"); // Splits the line into the 2 vertices
			if (parts[0].matches("[0-9]+") && parts[1].matches("[0-9]+")) { // Makes sure the vertices are numbers only
				int a = Integer.parseInt(parts[0]);
				int b = Integer.parseInt(parts[1]);
				if (a < b) { // to keep it uniform, checks which of the two node numbers is larger
					context.write(new Text(parts[0] + "_" + parts[1]), new Text("1")); // Emits key: a_b value: "1"
																						// where a<b
				} else {
					context.write(new Text(parts[1] + "_" + parts[0]), new Text("1")); // Emits key: b_a value: "1"
																						// where b<a
				}
			} else {
				throw new IllegalArgumentException("Input data format incorrect"); // Error in case of incorrect format
			}
		} else {
			throw new IllegalArgumentException("Input data format incorrect"); // Error in case of incorrect format
		}

	}

}
