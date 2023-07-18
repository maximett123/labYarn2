package com.opstty.mapper;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DistrictsContainingTrees extends Mapper<Object, Text, Text, NullWritable> {

    private Text district = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] fields = value.toString().split(";");

        // Assuming the district is the second field in the dataset
        district.set(fields[1]);
        context.write(district, NullWritable.get());
    }
}





