package com.opstty.mapper;

import com.opstty.TreeInfoWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OldestTreeMapper extends Mapper<Object, Text, IntWritable, TreeInfoWritable> {
    private final static IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(";");
        if (!fields[1].equals("ARRONDISSEMENT") && !fields[5].equals("ANNEE PLANTATION")) { // skip the header
            context.write(one, new TreeInfoWritable(Integer.parseInt(fields[1]), Integer.parseInt(fields[5])));
        }
    }
}
