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
 *** This is a sample program to chain the place filter job and replicated join job.
 *** 
 *** @author Jiaxi Li
 ***
 **/
public class JobChainDriver2 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("Usage: JobChainDriver <inPlace> <inPhoto> <out>");
            System.exit(2);
        }
        
        // Pre settings and clean the temporal path
        Path tmpFilterOut = new Path("tmpFilterOut"); // a temporary output path for the 1st job
        Path tmpPlacePhotoOut = new Path("tmpPlacePhotoOut"); // a temporary output path for the 2nd job
        Path tmpPhotoFreqOut = new Path("tmpPhotoFreqOut"); // a temporary output path for the 3rd job
        Path tmpSortedPhotoOut = new Path("tmpSortedPhotoOut"); // a temporary output path for the 4th job
        Path tmpTopTenOut = new Path("tmpTopTenOut"); // a temporary output path for the 5th job
        Path tmpUniqueOut = new Path("tmpUniqueOut");
        Path tmpFormatOut = new Path("tmpFormatPut");
        //Path tmp = new Path("tmpAll");

        FileSystem.get(conf).delete(tmpFilterOut, true);
        FileSystem.get(conf).delete(tmpPhotoFreqOut, true);
        FileSystem.get(conf).delete(tmpPlacePhotoOut, true);
        FileSystem.get(conf).delete(tmpTopTenOut, true);
        FileSystem.get(conf).delete(tmpSortedPhotoOut, true);
        FileSystem.get(conf).delete(tmpUniqueOut, true);
        FileSystem.get(conf).delete(tmpFormatOut, true);
        //FileSystem.get(conf).delete(tmp, true);


        PathFilter filter = new PathFilter(){
            public boolean accept(Path file){
                return file.getName().startsWith("part");
            }
        };
        
        // Job 1: filter the locality and neighbours
        Job placeFilterJob = new Job(conf, "Place Filter");
        placeFilterJob.setJarByClass(LocalityFilterMapper.class);
        placeFilterJob.setNumReduceTasks(0);
        placeFilterJob.setMapperClass(LocalityFilterMapper.class);
        placeFilterJob.setOutputKeyClass(Text.class);
        placeFilterJob.setOutputValueClass(Text.class);
        TextInputFormat.addInputPath(placeFilterJob, new Path(otherArgs[0]));
        TextOutputFormat.setOutputPath(placeFilterJob, tmpFilterOut);
        placeFilterJob.waitForCompletion(true);


        // Job 2: join the filtered with photo
        Job localityJoinJob = new Job(conf, "Place Photo Join");
        for (FileStatus fs : FileSystem.get(conf).listStatus(tmpFilterOut, filter)){
            DistributedCache.addCacheFile(fs.getPath().toUri(), localityJoinJob.getConfiguration());
        }
        localityJoinJob.setJarByClass(LocalityJoinMapper.class);
        localityJoinJob.setNumReduceTasks(0);
        localityJoinJob.setMapperClass(LocalityJoinMapper.class);
        localityJoinJob.setOutputKeyClass(Text.class);
        localityJoinJob.setOutputValueClass(Text.class);
        TextInputFormat.addInputPath(localityJoinJob, new Path(otherArgs[1]));
        TextOutputFormat.setOutputPath(localityJoinJob, tmpPlacePhotoOut);
        localityJoinJob.waitForCompletion(true);

        
        // Job 3: map/reduce job for locality frequency
        Job localityCountJob = new Job(conf, "Locality Count With Combiner");
        localityCountJob.setNumReduceTasks(3);
        localityCountJob.setJarByClass(LocalityFreqMapper.class);
        localityCountJob.setMapperClass(LocalityFreqMapper.class);
        localityCountJob.setReducerClass(LocalityFreqReducer.class);
        localityCountJob.setCombinerClass(LocalityFreqReducer.class);
        localityCountJob.setOutputKeyClass(Text.class);
        localityCountJob.setOutputValueClass(Text.class);
        
        for (FileStatus fs : FileSystem.get(conf).listStatus(tmpPlacePhotoOut, filter)){
            MultipleInputs.addInputPath(localityCountJob, fs.getPath(), TextInputFormat.class);
        }
        TextOutputFormat.setOutputPath(localityCountJob, tmpPhotoFreqOut);
        localityCountJob.waitForCompletion(true);


        // Job 4: sort job for locality in descending order and output the only top 10
        Job sortLocalityJob = new Job(conf, "Sort Locality Count");
        sortLocalityJob.setNumReduceTasks(1);
        sortLocalityJob.setJarByClass(LocalitySortMapper.class);
        sortLocalityJob.setMapperClass(LocalitySortMapper.class);
        sortLocalityJob.setGroupingComparatorClass(FreqComparator.class);
        sortLocalityJob.setSortComparatorClass(FreqComparator.class);
        sortLocalityJob.setReducerClass(LocalitySortReducer.class);
        sortLocalityJob.setOutputKeyClass(IntWritable.class);
        sortLocalityJob.setOutputValueClass(Text.class);
        
        for (FileStatus fs : FileSystem.get(conf).listStatus(tmpPhotoFreqOut, filter)){
            MultipleInputs.addInputPath(sortLocalityJob, fs.getPath(), TextInputFormat.class);
        }

        TextOutputFormat.setOutputPath(sortLocalityJob, tmpSortedPhotoOut);
        sortLocalityJob.waitForCompletion(true);


        //Job 5: Filter top 10 locality with neighours
        Job topFilterJob = new Job(conf, "Filter the top 10 locality with neibourhoods");
        for (FileStatus fs : FileSystem.get(conf).listStatus(tmpSortedPhotoOut, filter)){
            DistributedCache.addCacheFile(fs.getPath().toUri(), topFilterJob.getConfiguration());
        }

        topFilterJob.setJarByClass(PhotoFilterMapper.class);
        topFilterJob.setNumReduceTasks(0);
        topFilterJob.setMapperClass(PhotoFilterMapper.class);
        topFilterJob.setOutputKeyClass(Text.class);
        topFilterJob.setOutputValueClass(Text.class);
        for (FileStatus fs : FileSystem.get(conf).listStatus(tmpPlacePhotoOut, filter)){
            MultipleInputs.addInputPath(topFilterJob, fs.getPath(), TextInputFormat.class);
        }
        TextOutputFormat.setOutputPath(topFilterJob, tmpTopTenOut);
        topFilterJob.waitForCompletion(true);


        // Job 6: map/reduce number of unique users in the top10
        Job countUserJob = new Job(conf, "Count Number of Unique Users in Top 10");
        countUserJob.setNumReduceTasks(1);
        countUserJob.setJarByClass(UserCountMapper.class);
        countUserJob.setMapperClass(UserCountMapper.class);
        countUserJob.setGroupingComparatorClass(JoinGroupComparator.class);
        countUserJob.setReducerClass(UserCountReducer.class);
        countUserJob.setCombinerClass(UserCountReducer.class);
        countUserJob.setOutputKeyClass(Text.class);
        countUserJob.setOutputValueClass(Text.class);

        for (FileStatus fs : FileSystem.get(conf).listStatus(tmpTopTenOut, filter)){
            MultipleInputs.addInputPath(countUserJob, fs.getPath(), TextInputFormat.class);
        }
        TextOutputFormat.setOutputPath(countUserJob, tmpUniqueOut);
        countUserJob.waitForCompletion(true);


        // Job 7: map/reduce into country and formal format
        Job formatJob = new Job(conf, "Put in the correct format");
        formatJob.setNumReduceTasks(1);
        formatJob.setJarByClass(FormatMapper.class);
        formatJob.setMapperClass(FormatMapper.class);
        formatJob.setReducerClass(FormatReducer.class);
        formatJob.setCombinerClass(FormatReducer.class);
        formatJob.setOutputKeyClass(Text.class);
        formatJob.setOutputValueClass(Text.class);
        
        for (FileStatus fs : FileSystem.get(conf).listStatus(tmpUniqueOut, filter)){
            MultipleInputs.addInputPath(formatJob, fs.getPath(), TextInputFormat.class);
        }
        TextOutputFormat.setOutputPath(formatJob, tmpFormatOut);
        formatJob.waitForCompletion(true);
        
        // Job: Last job to produce the correct output
        Job finalJob = new Job(conf, "Final Job to produce the correct output");
        finalJob.setJarByClass(FormatJoinMapper.class);
        finalJob.setNumReduceTasks(0);
        finalJob.setMapperClass(FormatJoinMapper.class);
        finalJob.setOutputKeyClass(Text.class);
        finalJob.setOutputValueClass(Text.class);
        
        for (FileStatus fs : FileSystem.get(conf).listStatus(tmpFormatOut, filter)){
            MultipleInputs.addInputPath(finalJob, fs.getPath(), TextInputFormat.class);
        }

        TextOutputFormat.setOutputPath(finalJob, new Path(otherArgs[2]));
        finalJob.waitForCompletion(true);
        




















    }
}
