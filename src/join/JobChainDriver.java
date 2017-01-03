package join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.FileStatus;

/**
 ** This is a sample program to chain the place filter job and replicated join job.
 ** 
 ** @author Jiaxi Li
 **
 **/

public class JobChainDriver {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 3) {
			System.err.println("Usage: JobChainDriver <inPlace> <inPhoto> <out>");
			System.exit(2);
		}
		
		Path tmpFilterOut = new Path("tmpFilterOut"); // a temporary output path for the 1st job
		Path tmpPlacePhotoOut = new Path("tmpPlacePhotoOut"); // a temporary output path for the 2nd job
		Path tmpPhotoFreqOut = new Path("tmpPhotoFreqOut"); // a temporary output path for the 3rd job
		Path tmpSortedPhotoOut = new Path("tmpSortedPhotoOut"); // a temporary output path for the 4th job
		Path tmpTagFreqOut = new Path("tmpTagFreqOut"); // a temporary output path for the 5th job
		Path tmpSortedTagOut = new Path("tmpSortedTagOut"); // a temporary output path for the 6th job

		FileSystem.get(conf).delete(tmpFilterOut, true);
		FileSystem.get(conf).delete(tmpPhotoFreqOut, true);
		FileSystem.get(conf).delete(tmpPlacePhotoOut, true);
		FileSystem.get(conf).delete(tmpTagFreqOut, true);
		FileSystem.get(conf).delete(tmpSortedPhotoOut, true);
		FileSystem.get(conf).delete(tmpSortedTagOut, true);
		
		// the path filter
		PathFilter filter = new PathFilter(){
                        public boolean accept(Path file){
                                return file.getName().startsWith("part");
                        }
                };
	
	
		// pass a parameter to mapper class
		//conf.set("join.placeName", otherArgs[3]);	
		
		// Job 1: Place filter
		Job placeFilterJob = new Job(conf, "Place Filter");
		placeFilterJob.setJarByClass(PlaceFilterDriver.class);
		placeFilterJob.setNumReduceTasks(0);
		placeFilterJob.setMapperClass(PlaceFilterMapper.class);
		placeFilterJob.setOutputKeyClass(Text.class);
		placeFilterJob.setOutputValueClass(Text.class);
		TextInputFormat.addInputPath(placeFilterJob, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(placeFilterJob, tmpFilterOut);
		placeFilterJob.waitForCompletion(true);
		//FileSystem.get(conf).copyToLocalFile(tmpFilterOut, new Path("~/CloudComputing/a1/"));
		
		// Job 2: Place photo join
		Job placePhotoJoinJob = new Job(conf, "Place Photo Join");
		for (FileStatus fs : FileSystem.get(conf).listStatus(tmpFilterOut, filter)){
			DistributedCache.addCacheFile(fs.getPath().toUri(), placePhotoJoinJob.getConfiguration());
                }
		DistributedCache.addCacheFile(tmpFilterOut.toUri(),placePhotoJoinJob.getConfiguration());
		//conf.set("join.place.table", tmpFilterOut + "/part-m-00000");
		placePhotoJoinJob.setJarByClass(PlacePhotoJoinDriver.class);
		placePhotoJoinJob.setNumReduceTasks(0);
		placePhotoJoinJob.setMapperClass(PlacePhotoJoinMapper.class);
		placePhotoJoinJob.setOutputKeyClass(Text.class);
		placePhotoJoinJob.setOutputValueClass(Text.class);
		TextInputFormat.addInputPath(placePhotoJoinJob, new Path(otherArgs[1]));
		TextOutputFormat.setOutputPath(placePhotoJoinJob, tmpPlacePhotoOut);
		placePhotoJoinJob.waitForCompletion(true);


		// Job 3: Photo frequency map/reduce
		Job photoCountJob = new Job(conf, "Photo Count With Combiner");
		photoCountJob.setNumReduceTasks(3);
		photoCountJob.setJarByClass(PhotoFreqDriver.class);
		photoCountJob.setMapperClass(PhotoFreqMapper.class);
		photoCountJob.setReducerClass(PhotoFreqReducer.class);
		photoCountJob.setCombinerClass(PhotoFreqReducer.class);
		photoCountJob.setOutputKeyClass(Text.class);
		photoCountJob.setOutputValueClass(Text.class);

		// add all inputs
   		for (FileStatus fs : FileSystem.get(conf).listStatus(tmpPlacePhotoOut, filter)){
			MultipleInputs.addInputPath(photoCountJob, fs.getPath(), TextInputFormat.class);
	  	}
		TextOutputFormat.setOutputPath(photoCountJob, tmpPhotoFreqOut);
		photoCountJob.waitForCompletion(true);

		// Job 4: Sort photo count
		Job sortPhotoCountJob = new Job(conf, "Sort Photo Freq");
		//TextInputFormat.addInputPath(sortPhotoCountJob, tmpPhotoFreqOut);
        	TextOutputFormat.setOutputPath(sortPhotoCountJob, tmpSortedPhotoOut);
        	sortPhotoCountJob.setNumReduceTasks(1);
        	sortPhotoCountJob.setJarByClass(SortFreqDriver.class);
        	sortPhotoCountJob.setMapperClass(SortFreqMapper.class);
        	sortPhotoCountJob.setGroupingComparatorClass(FreqComparator.class);
        	sortPhotoCountJob.setSortComparatorClass(FreqComparator.class);
		sortPhotoCountJob.setReducerClass(SortFreqReducer.class);
        	sortPhotoCountJob.setOutputKeyClass(IntWritable.class);
        	sortPhotoCountJob.setOutputValueClass(Text.class);
		
		for (FileStatus fs : FileSystem.get(conf).listStatus(tmpPhotoFreqOut, filter)){
                        MultipleInputs.addInputPath(sortPhotoCountJob, fs.getPath(), TextInputFormat.class);
                }
		sortPhotoCountJob.waitForCompletion(true);

		// Job 5: Tag frequency map/reduce
		Job tagCountJob = new Job(conf, "Tag Count With Combiner");
		tagCountJob.setNumReduceTasks(3);
        	tagCountJob.setJarByClass(TagDriver.class);
        	tagCountJob.setMapperClass(TagMapper.class);
        	tagCountJob.setGroupingComparatorClass(FreqComparator.class);
		tagCountJob.setReducerClass(TagReducer.class);
        	tagCountJob.setCombinerClass(TagReducer.class);
        	tagCountJob.setOutputKeyClass(Text.class);
        	tagCountJob.setOutputValueClass(Text.class);
        	//TextInputFormat.addInputPath(tagCountJob, tmpPlacePhotoOut);
        	for (FileStatus fs : FileSystem.get(conf).listStatus(tmpPlacePhotoOut, filter)){
                        MultipleInputs.addInputPath(tagCountJob, fs.getPath(), TextInputFormat.class);
                }
		TextOutputFormat.setOutputPath(tagCountJob, tmpTagFreqOut);
        	tagCountJob.waitForCompletion(true);

		// Job 6: Sort tag count and leave the highest 10 tags if exist
        	Job sortTagCountJob = new Job(conf, "Sort Tag Frequency");
        	sortTagCountJob.setNumReduceTasks(0);
        	sortTagCountJob.setJarByClass(TagSortDriver.class);
        	sortTagCountJob.setMapperClass(TagSortMapper.class);
        	sortTagCountJob.setOutputKeyClass(Text.class);
        	sortTagCountJob.setOutputValueClass(Text.class);
        	//TextInputFormat.addInputPath(sortTagCountJob, tmpTagFreqOut);
        	for (FileStatus fs : FileSystem.get(conf).listStatus(tmpTagFreqOut, filter)){
                        MultipleInputs.addInputPath(sortTagCountJob, fs.getPath(), TextInputFormat.class);
                }
		TextOutputFormat.setOutputPath(sortTagCountJob, tmpSortedTagOut);
        	sortTagCountJob.waitForCompletion(true);
	

        	// Job 7: Final Job --- join sorted photo frequency and tag frequency
		Job finalJoin = new Job(conf, "Final Join");
		for (FileStatus fs : FileSystem.get(conf).listStatus(tmpSortedTagOut, filter)){
			DistributedCache.addCacheFile(fs.getPath().toUri(),finalJoin.getConfiguration());;
                }
        	//conf.set("join.final.tagTable", tmpFiles);
       		finalJoin.setJarByClass(JoinDriver.class);
        	finalJoin.setNumReduceTasks(0);
        	finalJoin.setMapperClass(JoinMapper.class);
        	finalJoin.setOutputKeyClass(Text.class);
        	finalJoin.setOutputValueClass(Text.class);
        	TextInputFormat.addInputPath(finalJoin, tmpSortedPhotoOut);
        	TextOutputFormat.setOutputPath(finalJoin, new Path(otherArgs[2]));
 	      	finalJoin.waitForCompletion(true);

		//FileSystem.get(conf).delete(tmpFilterOut, true);
		//FileSystem.get(conf).delete(tmpPhotoFreqOut, true);
		//FileSystem.get(conf).delete(tmpPlacePhotoOut, true);
		//FileSystem.get(conf).delete(tmpTagFreqOut, true);
		//FileSystem.get(conf).delete(tmpSortedPhotoOut, true);
		//FileSystem.get(conf).delete(tmpSortedTagOut, true);
	}
}

