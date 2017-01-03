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


public class LocalityJoinMapper extends Mapper<Object, Text, Text, Text> {
	private Hashtable <String, String> placeTable = new Hashtable<String, String>();
	private Text keyOut = new Text(), valueOut = new Text();

	public void setPlaceTable(Hashtable<String,String> place){
		placeTable = place;
	}
	
	public void setup(Context context)
		throws java.io.IOException, InterruptedException{
		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		if (cacheFiles != null && cacheFiles.length > 0){
			String line;
			String[] tokens;
			BufferedReader placeReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
			try {
				String tok = "";
				while ((line = placeReader.readLine()) != null) {
					tokens = line.split("\t");
					tok = tokens[1] + "\t" + tokens[2];
					placeTable.put(tokens[0], tok); // use full place.txt index is 6, other wise it is 1.
				}
			} 
			finally {
				placeReader.close();
			}
		}
	}
	
	public void map(Object key, Text value, Context context
	) throws IOException, InterruptedException {
		String[] dataArray = value.toString().split("\t"); //split the data into array
		if (dataArray.length < 5){ // a not complete record with all data
			return; // don't emit anything
		}
		String placeId = dataArray[4];
		String places = placeTable.get(placeId);
		if (places != null){
			String[] sep = places.split("\t");
			String placeName = sep[0];
			String placeType = sep[1];
			
			String locality = placeName;		
			
			if(placeType.equals("22")){
				locality = placeName.substring(0, placeName.lastIndexOf("/"));
            } else{
			}
		
			String user = dataArray[1];
			
			keyOut.set(placeName);
			valueOut.set(locality + "\t" + user);
			context.write(keyOut, valueOut);
		}
		
	}

}

