package edu.stevens.cs549.hadoop.socialrank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffMap1 extends Mapper<LongWritable, Text, Text, Text> {

	/* TODO: Your mapper code here */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		String[] sections = line.split("\t"); // Splits each line
		if (sections.length > 2) // checks for incorrect data format
		{
			throw new IOException("Incorrect data format");
		}
		String[] node_rank = sections[0].split(";"); // Splits the node;rank pair

		context.write(new Text(node_rank[0]), new Text(node_rank[1])); // Outputs: key: node value: rank

	}

}
