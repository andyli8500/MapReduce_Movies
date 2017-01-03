package join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.IntWritable;

public class SortFreqDriver {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: JobChainDriver <in> <out>");
			System.exit(2);
		}

		Job sortJob = new Job(conf, "sort freq");
		TextInputFormat.addInputPath(sortJob, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(sortJob, new Path(otherArgs[1]));
		sortJob.setNumReduceTasks(1);
		sortJob.setJarByClass(SortFreqDriver.class);
		sortJob.setMapperClass(SortFreqMapper.class);
		sortJob.setGroupingComparatorClass(FreqComparator.class);
		//sortjob.setSortComparatorClass(DescendingKeyComparator.class);
		sortJob.setReducerClass(SortFreqReducer.class);
		sortJob.setOutputKeyClass(IntWritable.class);
		//sortJob.setOutputValueClass(Text.class);
		System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
	}
}
