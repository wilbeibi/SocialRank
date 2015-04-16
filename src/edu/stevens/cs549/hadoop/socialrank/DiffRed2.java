package edu.stevens.cs549.hadoop.socialrank;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffRed2 extends Reducer<Text, Text, Text, Text> {

	/* TODO: Your reducer code here */
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double diff_max = 0.0; // sets diff_max to a default value
		int i = 0;
		for (Text v : values) {
			if (i == 0) {
				// For first iteration, sets diff_max to the first difference in the iterable values
				diff_max = Double.parseDouble(v.toString());
				i++;
				continue;
			}
			double val = Double.parseDouble(v.toString()); // Converts String to a double
			if (val > diff_max) {
				// If difference is greater than the current max, set new larger difference to diff_max
				diff_max = val;
			}
		}

		System.out.println(diff_max);
		context.write(new Text(diff_max + ""), new Text()); // emits: diff_max

	}
}
