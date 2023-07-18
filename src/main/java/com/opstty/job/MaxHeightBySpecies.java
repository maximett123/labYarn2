package com.opstty.job;

import com.opstty.mapper.MaxHeightBySpeciesMapper;
import com.opstty.reducer.MaxHeightBySpeciesReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MaxHeightBySpecies {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "max height by species");

        job.setJarByClass(MaxHeightBySpecies.class);
        job.setMapperClass(MaxHeightBySpeciesMapper.class);
        job.setCombinerClass(MaxHeightBySpeciesReducer.class);
        job.setReducerClass(MaxHeightBySpeciesReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

