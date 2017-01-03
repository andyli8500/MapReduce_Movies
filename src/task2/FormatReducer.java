package join;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class FormatReducer extends Reducer<Text, Text, Text, Text> {
    Text result = new Text();
    
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer strBuf = new StringBuffer();
        StringBuffer allUser = new StringBuffer();
        Set<String> uniqueUsers = new HashSet<String>();


        for (Text text: values){
            String[] data = text.toString().split("\t");
            strBuf.append(data[0] + ",");
            String[] users = data[1].split(",");
            for(String user : users){
                if(user.length() < 2)
                    continue;
                uniqueUsers.add(user);
            }        
        }
    
        for(String u : uniqueUsers)
            allUser.append(u + ",");

        result.set(strBuf.toString() + "\t" + allUser.toString());
        context.write(key, result);

    }
}

