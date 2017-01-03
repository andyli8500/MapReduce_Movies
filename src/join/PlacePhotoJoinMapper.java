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

/**
 * This is an example of using DistributedCache to join a large table with
 * a small one.
 * 
 * The files to be distributed are setup in the driver method
 * 
 * In the Mapper's setup method, we read the file and store its content in
 * desirable structure as the mapper's instance variable, which stays in the 
 * memory during calls of the map methods.
 * 
 * In this particular example, the file content is just a key value pair of
 * place_id and place_name. We use a hashtable to store it.
 * 
 * The Mapper's input is one of n0x.txt, representing photo information. 
 * 
 * input record format
 * 2048252769	48889082718@N01	dog francis lab	2007-11-19 17:49:49	RRBihiubApl0OjTtWA	16
 * 
 * output record format:
 * 
 * photo_id \t date_taken \t place_name
 * 
 * 
 * @see ReplicateJoinDriver
 * @author Ying Zhou
 *
 */
public class PlacePhotoJoinMapper extends Mapper<Object, Text, Text, Text> {
	private Hashtable <String, String> placeTable = new Hashtable<String, String>();
	private Text keyOut = new Text(), valueOut = new Text();

	public void setPlaceTable(Hashtable<String,String> place){
		placeTable = place;
	}
	
	// get the distributed file and parse it
	public void setup(Context context)
		throws java.io.IOException, InterruptedException{
		//String path = context.getConfiguration().get("join.place.table");
		//Path cacheFiles = new Path(path);
		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		if (cacheFiles != null && cacheFiles.length > 0){
			String line;
			String[] tokens;
			BufferedReader placeReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
			try {
				String tok = "";
				while ((line = placeReader.readLine()) != null) {
					tokens = line.split("\t");
					tok = tokens[1] + "/" + tokens[2];
					placeTable.put(tokens[0], tok); // use full place.txt index is 6, other wise it is 1.
				}
				//System.out.println("size of the place table is: " + placeTable.size());
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
		String year = dataArray[3].split("-")[0];
		if (places != null){
			String[] sep = places.split("/");
			String placeName = sep[0];
			String placeType = sep[1];
			String tagString = dataArray[2];
			
			String[] placeTmp = placeName.toLowerCase().replaceAll(" ", "").split(",");
			String usefulTag = "";

			// filter out the useful tags
			String[] tagArray = tagString.split(" ");
			Boolean useful;
			for (String tag : tagArray){
				if (tag.contains(year))
					continue;
				useful = true;
				for(String pls : placeTmp){
					if (pls.equals(tag.toLowerCase())){
						useful = false;
						break;
					}	
				}
				if (useful == true)
					usefulTag += tag + " ";
			}
			
			// keep the locality level t simplify the work
			// if the place is in the neibourhood, replace it with locality
			if (placeType.equals("22")){
				placeName = placeName.substring(placeName.indexOf(",") + 2);
			}
			
			valueOut.set(usefulTag);
			keyOut.set(placeName);
			context.write(keyOut, valueOut);
		}
		
	}

}
