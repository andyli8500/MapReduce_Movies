package join;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class PhotoFreqReducer extends Reducer<Text, Text, Text, Text> {
	Text result = new Text();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		int sum = 0;
		for (Text text: values){
			String freqString = text.toString();
		
			int freqCount = Integer.parseInt(freqString);
			sum += freqCount;
				
		}

		result.set(Integer.toString(sum));
		context.write(key, result);
	}
}

