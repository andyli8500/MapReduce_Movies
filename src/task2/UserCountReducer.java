package join;

import java.io.IOException;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class UserCountReducer extends Reducer<Text, Text, Text, Text> {
    Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> uniqueUser = new HashSet<String>();
        for (Text text: values){
            String[] users = text.toString().split(",");
            for(String user : users)
                uniqueUser.add(user);
        }
        
        StringBuffer strBuf = new StringBuffer();
        for(String u : uniqueUser){
            strBuf.append(u + ",");
        }
        result.set(strBuf.toString());
        context.write(key, result);
    }
}


