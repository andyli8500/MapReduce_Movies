package join;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.FileReader;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;


public class PhotoFilterMapper extends Mapper<Object, Text, Text, Text> {
    private Text placeName = new Text(), other = new Text();
    private ArrayList<String> top10 = new ArrayList<String>();

    public void setup(Context context) throws java.io.IOException, InterruptedException{
        
        Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        if(cacheFiles != null && cacheFiles.length > 0) {
            for(Path cacheFile : cacheFiles){
                String line;
                String[] tokens;
                BufferedReader placeReader = new BufferedReader(new FileReader(cacheFile.toString()));
                
                try {
                    while ((line = placeReader.readLine()) != null) {
                        tokens = line.split("\t");
                        top10.add(tokens[0]); 
                    }
                }
                finally {
                    placeReader.close();
                }
            }
        }
    
    }
    
    public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
        
        String[] dataArray = value.toString().split("\t"); 
        if (dataArray.length < 3){ 
            return; 
        }
        
        String locality = dataArray[1];
        for (String loc : top10){
            if(loc.equals(locality)){
                placeName.set(dataArray[0]);
                other.set(dataArray[1] + "\t" + dataArray[2]);
                context.write(placeName, other);
            }
        }

    }
}
