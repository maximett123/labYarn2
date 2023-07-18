package com.opstty.job;

import com.opstty.mapper.SortTreesByHeightMapper;
import com.opstty.reducer.SortTreesByHeightReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SortTreesByHeight {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "sort trees by height");

        job.setJarByClass(SortTreesByHeight.class);
        job.setMapperClass(SortTreesByHeightMapper.class);
        job.setReducerClass(SortTreesByHeightReducer.class);

        job.setOutputKeyClass(FloatWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
