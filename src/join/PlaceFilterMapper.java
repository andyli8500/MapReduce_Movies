package join;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * This mapper is used to filter place names in locality level
 * 
 * input format:
 * place_id \t woeid \t latitude \t longitude \t place_name \t place_type_id \t place_url
 * 
 * output format:
 * place_id \t place_name
 * 
 * The country name is stored as a property in the job's configuration object.
 * 
 */
public class PlaceFilterMapper extends Mapper<Object, Text, Text, Text> {
	private Text placeId= new Text(), locality = new Text();
	
	public void setup(Context context){
		//countryName = context.getConfiguration().get("mapper.placeFilter.country", countryName);
	}
	
	public void map(Object key, Text value, Context context
	) throws IOException, InterruptedException {
		String[] dataArray = value.toString().split("\t"); //split the data into array
		if (dataArray.length < 7){ // a not complete record with all data
			return; // don't emit anything
		}
		String placeType = dataArray[5].replaceAll("\\s+","");
		if (placeType.equals("7") || placeType.equals("22")){
			//placeId.set(placeType);
			placeId.set(dataArray[0]);
			String place = dataArray[4];
			locality.set(place + "\t" + placeType);
			context.write(placeId, locality);
		} 
		
	}

}
