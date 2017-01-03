package join;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class FormatJoinMapper extends Mapper<Object, Text, Text, Text> {
    private Text keyOut = new Text(), valueOut = new Text();
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] dataArray = value.toString().split("\t"); 
        if (dataArray.length < 3){ 
            return; 
        }

        String[] users = dataArray[2].split(",");
        String numUsers = String.valueOf(users.length);
        
        String[] place = dataArray[0].split("/");
        String locality = place[place.length-1];
        String country = place[0];
        valueOut.set(locality + "=" + numUsers + "\t" + dataArray[1]);
        keyOut.set(country); 

        context.write(keyOut, valueOut);
    }
}
