package join;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.fs.FileStatus;
import java.io.FileNotFoundException;

public class JoinMapper extends Mapper<Object, Text, Text, Text> {
	private Hashtable <String, String> plaTable = new Hashtable<String, String>();
	private Text keyOut = new Text(), valueOut = new Text();
	private int counter = 0;
	public void setPlaceTable(Hashtable<String,String> place){
		plaTable = place;
	}
	
	public void setup(Context context)
		throws java.io.IOException, InterruptedException{
		
		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		if(cacheFiles != null && cacheFiles.length > 0) {
			for(Path cacheFile : cacheFiles){	
				String line;
           			String[] tokens;
            			BufferedReader placeReader = new BufferedReader(new FileReader(cacheFile.toString()));
            			try {
            				while ((line = placeReader.readLine()) != null) {
                				tokens = line.split("\t");
                    				plaTable.put(tokens[0], tokens[1]); 
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
		if (dataArray.length < 2){ 
			return; 
		}
		if(counter >= 50)
			return;

		String placeName = dataArray[0];
        	String tags = plaTable.get(placeName);

		if (tags != null){
			valueOut.set(dataArray[1] + "\t" + tags);
		        keyOut.set(placeName);
			context.write(keyOut, valueOut);
			counter++;
		}
		
	}

}

