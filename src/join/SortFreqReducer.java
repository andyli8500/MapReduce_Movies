package join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.io.IntWritable;

public class SortFreqReducer extends Reducer<IntWritable, Text, Text, Text> {
	private Text keyOut = new Text(), valOut = new Text();
	
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text val : values) {
			keyOut.set(val.toString());
			valOut.set(key.toString());
			context.write(keyOut, valOut);
		}
	}
}

