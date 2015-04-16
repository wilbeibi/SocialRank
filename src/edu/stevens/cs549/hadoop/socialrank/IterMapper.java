package edu.stevens.cs549.hadoop.socialrank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class IterMapper extends Mapper<LongWritable, Text, Text, Text> {

	/* TODO: Your mapper code here */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		String[] sections = line.split("\t"); // Splits it into two parts. Part 1: node;rank | Part 2:
												// friend1,friend2...,friendn
		if (sections.length > 2) // Checks if the data is in the incorrect format
		{
			throw new IOException("Incorrect data format");
		}
		if (sections.length != 2) {
			return;
		}
		String[] nodes = sections[1].split(","); // Splits the friend1,friend2...,friendn into each individual friend
													// and stores it in an array
		String[] host_rank = sections[0].split(";"); // Splits the node;rank part and stores it in an array
		double rank = Double.parseDouble(host_rank[1]); // Stores the rank as a double
		int number = nodes.length; // Number of friends

		double weight = (rank / number); // Weight as per SocialRank algorithm

		String val = weight + ""; // Stores "weight" in a string
		for (int i = 0; i < number; i++) // Emits each friend and the weight couple: key: friendx value: node;weight
		{
			context.write(new Text(nodes[i]), new Text(val));
		}

		context.write(new Text(host_rank[0]), new Text("!" + sections[1])); // Writes out key: node Value:
																			// friend1,friend2,...,friendn

	}

}
