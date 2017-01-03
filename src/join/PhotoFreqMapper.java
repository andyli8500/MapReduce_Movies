package join;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class PhotoFreqMapper extends Mapper<Object, Text, Text, Text> {
	private Text place = new Text();
	
	public void map(Object key, Text value, Context context
	) throws IOException, InterruptedException {
		String[] dataArray = value.toString().split("\t");
		if (dataArray.length < 2){ 
			return;
		}

		place.set(dataArray[0]);
		context.write(place, new Text("1"));
	}
}

