package join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * This is a sample program to configure a map only job to
 * filter place data based on input countryName
 * 
 * The country name is passed as a command line argument and is 
 * stored as a property in the job's configuration object. 
 * 
 * The property "mapper.placeFilter.country" can be read out
 * by both Mapper and Reducer through their context objects.
 * 
 * @see PlaceFilterMapper
 * @author Ying Zhou
 *
 */
public class PlaceFilterDriver {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			//System.err.println("Usage: PlaceFilter <in> <out> [countryName]");
		        System.err.println("Usage: PlaceFilter <in> <out> ");
			System.exit(2);
		}
		if (otherArgs.length == 3){
			conf.set("mapper.placeFilter.country", otherArgs[2]);
		}
		Job job = new Job(conf, "Place Filter");
		job.setJarByClass(PlaceFilterDriver.class);
		job.setNumReduceTasks(0);
		job.setMapperClass(PlaceFilterMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
