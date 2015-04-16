package edu.stevens.cs549.hadoop.socialrank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class IterReducer extends Reducer<Text, Text, Text, Text> {

	/* TODO: Your reducer code here */
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double d = 0.15; // Decay factor
		double rank = 0.0; // stores the decay factor in a variable rank

		LinkedList<String> vals = new LinkedList<String>(); // Linked list to store all the "friend;weight" pairs
		for (Text v : values) // Iterates over the values
		{
			vals.add(v.toString()); // adds each weight/(friends list) to vals
		}

		String friend_list = "";
		double sum_weight = 0.0;
		for (String v : vals) // iterates over vals
		{
			if (!v.contains("!")) {
				double weight = Double.parseDouble(v); // Stores the weight as a double
				sum_weight = sum_weight + (weight); // Calculates rank iteratively
			} else {
				friend_list = v.substring(1);
			}
		}
		rank = d + (1 - d) * sum_weight;

		context.write(new Text(key.toString() + ";" + rank), new Text(friend_list)); // output: node;rank
																						// friend1,friend2,....,friendn

	}
}
