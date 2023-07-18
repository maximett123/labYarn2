package com.opstty.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxHeightBySpeciesMapper extends Mapper<Object, Text, Text, FloatWritable> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(";");
        if (!fields[3].equals("ESPECE") && !fields[6].equals("HAUTEUR")) { // skip the header
            context.write(new Text(fields[3]), new FloatWritable(Float.parseFloat(fields[6])));
        }
    }
}

