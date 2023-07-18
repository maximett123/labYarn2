package com.opstty.job;

import com.opstty.mapper.DistrictsContainingTrees;
import com.opstty.reducer.DistrictsContainingTreesReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class UniqueDistricts {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "unique districts");

        job.setJarByClass(UniqueDistricts.class);
        job.setMapperClass(DistrictsContainingTrees.class);
        job.setCombinerClass(DistrictsContainingTreesReduce.class);
        job.setReducerClass(DistrictsContainingTreesReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setNumReduceTasks(1); // This will ensure a single reducer is used.

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
