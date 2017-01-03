package join;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class LocalityFreqMapper extends Mapper<Object, Text, Text, Text> {
    private Text locality = new Text();
    

    public void setup(Context context) {
    }

    public void map(Object key, Text value, Context context) 
        throws IOException, InterruptedException {
        String[] dataArray = value.toString().split("\t");

        if(dataArray.length < 2){
            return;
        }

        locality.set(dataArray[1]);
        context.write(locality, new Text("1"));
    }

}
