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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class TagSortMapper extends Mapper<Object, Text, Text, Text> {
    private Text place = new Text(), tags = new Text();
    
    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        
        String[] dataArray = value.toString().split("\t"); 
        if (dataArray.length < 2){ 
            return; 
        }
        
        String[] tagArray = dataArray[1].split(",");
        String placeString = dataArray[0];

        Map<String, Integer> tagFrequency = new HashMap<String,Integer>();

        for(String tag : tagArray){
            String[] tagData = tag.split("::");
            String tagName = tagData[0];

            try{
                int tagCount = Integer.parseInt(tagData[1]);
                if (tagFrequency.containsKey(tagName)){
                    tagFrequency.put(tagName, tagFrequency.get(tagName) + tagCount);
                }else{
                    tagFrequency.put(tagName, tagCount);
                }
            }catch (NumberFormatException e){
            }
        }

        StringBuffer strBuf = new StringBuffer();

        Set<Entry<String, Integer>> set = tagFrequency.entrySet();
        List<Entry<String, Integer>> list = new ArrayList<Entry<String, Integer>>(set);
        Collections.sort( list, new Comparator<Map.Entry<String, Integer>>()
        {
            public int compare( Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2 )
            {
                return (o2.getValue()).compareTo( o1.getValue() );
            }
        } );
	
	int index = 0;
        for(Map.Entry<String, Integer> entry : list){
		if (index > 9)
			break;
		index++;
        	strBuf.append(entry.getKey()+"::"+entry.getValue()+",");
        }

        place.set(placeString);
        tags.set(strBuf.toString());
        context.write(place, tags);
    }
}

