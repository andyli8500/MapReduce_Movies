package join;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class UserCountMapper extends Mapper<Object, Text, Text, Text> {
    private Text place = new Text(), users = new Text();

    public void map(Object key, Text value, Context context
            ) throws IOException, InterruptedException {
        String[] dataArray = value.toString().split("\t");
        if (dataArray.length < 3){ 
            return;
        }

        place.set(dataArray[0]);
        users.set(dataArray[2] + ",");
        context.write(place, users);
    }
}


