package edu.stevens.cs549.hadoop.socialrank;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffRed1 extends Reducer<Text, Text, Text, Text> {

	/* TODO: Your reducer code here */
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double[] ranks = new double[2];
		int i = 0;
		for (Text v : values) {
			// Iterates over values, there should be only 2 ranks for each node
			if (i < 2) { // checks for case for more than two ranks
				ranks[i] = Double.parseDouble(v.toString()); // stores the ranks
			} else {
				throw new IOException("More than two Rank values for one key"); // throws error
			}
			i++;
		}

		double difference = Math.abs(ranks[0] - ranks[1]); // Calculates the difference

		context.write(new Text(difference + ""), new Text()); // emits: difference

	}
}
