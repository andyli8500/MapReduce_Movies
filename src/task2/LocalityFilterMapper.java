package join;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LocalityFilterMapper extends Mapper<Object, Text, Text, Text> {
	private String type = "7"; // Default Locality Type
	private Text placeId= new Text(), locality = new Text();
	
	public void setup(Context context){
       		type = context.getConfiguration().get("mapper.placeFilter.type", type);
	}

	public void map(Object key, Text value, Context context
	) throws IOException, InterruptedException {
		String[] dataArray = value.toString().split("\t"); 
		if (dataArray.length < 7){ 
			return; 
		}
		

		String placeType = dataArray[5].replaceAll("\\s+","");
		if (placeType.equals("7") || placeType.equals("22")){
			placeId.set(dataArray[0]);
			String placeUrl = dataArray[6];
			locality.set(placeUrl + "\t" + placeType);
			context.write(placeId, locality);
		} 
		
	}

}

