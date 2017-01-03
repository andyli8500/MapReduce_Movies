package join;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.Comparator;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class TagReducer extends Reducer<Text, Text, Text, Text> {
	Text result = new Text();
	
	public void reduce(Text key, Iterable<Text> values, 
			Context context
	) throws IOException, InterruptedException {

		Map<String, Integer> tagFrequency = new HashMap<String,Integer>();
		
		for (Text text: values){
			String dataString = text.toString();
			
			String[] dataArray = dataString.split(",");
			for (int i = 0; i < dataArray.length; i++){
				
				String[] tagData =dataArray[i].split("::");
				String tagName = tagData[0];

				try{
					int tagCount = Integer.parseInt(tagData[1]);
					if (tagFrequency.containsKey(tagName)){
						tagFrequency.put(tagName, tagFrequency.get(tagName) + tagCount);
					}else{
						tagFrequency.put(tagName, tagCount);
					}
				}catch (NumberFormatException e){
					System.out.println(text.toString());
				}
			}
		}

		StringBuffer strBuf = new StringBuffer();
		for (String tagName: tagFrequency.keySet()){
			strBuf.append(tagName + "::"+tagFrequency.get(tagName)+",");
		}
		result.set(strBuf.toString());
		context.write(key, result);
	}
	
}

