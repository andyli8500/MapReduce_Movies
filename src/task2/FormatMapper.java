package join;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class FormatMapper extends Mapper<Object, Text, Text, Text> {
    private Text country = new Text(), others = new Text();
            
    public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
        String[] dataArray = value.toString().split("\t");
        if (dataArray.length < 2){ 
            return;
        }
        
        String tmp = dataArray[0];
        String[] place = tmp.replaceFirst("/", "").split("/");

        if(place.length < 3)
            return;
        
        String countryName = place[0]+"/"+place[1]+"/"+place[2]; 
        String neighborName = place[place.length - 1];

        String numUser = String.valueOf(dataArray[1].split(",").length);
        
        country.set(countryName);
        
        others.set(neighborName + "=" + numUser + "\t" + dataArray[1]);
        context.write(country, others);
        
        }
}

