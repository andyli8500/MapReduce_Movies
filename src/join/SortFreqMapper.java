package join;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.IntWritable;

public class SortFreqMapper extends Mapper<Object, Text, IntWritable, Text> {
    private Text place = new Text();
    private IntWritable freq = new IntWritable();
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] dataArray = value.toString().split("\t");
        if (dataArray.length < 2){ 
            return;
        }
        place.set(dataArray[0]);
        try{
            freq.set(Integer.parseInt(dataArray[1]));

            context.write(freq, place);
        } catch(NumberFormatException e){
            return;
        }
    }
}

