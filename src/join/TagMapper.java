package join;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class TagMapper extends Mapper<Object, Text, Text, Text> {
	private Text place = new Text(), tags = new Text();
	static CharsetEncoder asciiEncoder = Charset.forName("US-ASCII").newEncoder(); 
	
	public void map(Object key, Text value, Context context
	) throws IOException, InterruptedException {
		
		String[] dataArray = value.toString().split("\t"); 
		if (dataArray.length < 2){ 
			return; 
		}
		
		String tagString = dataArray[1];
		String placeString = dataArray[0];

		if (tagString.length() > 0){
			String[] tagArray = tagString.split(" ");
			for(String tag: tagArray) {
				if (asciiEncoder.canEncode(tag)){
					place.set(placeString);
					tags.set(tag+"::1,");
					context.write(place, tags);
				}
			}
		}
	}
}

