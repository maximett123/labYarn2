package com.opstty.reducer;

import com.opstty.TreeInfoWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OldestTreeReducer extends Reducer<IntWritable, TreeInfoWritable, IntWritable, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<TreeInfoWritable> values, Context context)
            throws IOException, InterruptedException {
        int oldestYear = Integer.MAX_VALUE;
        int oldestDistrict = 0;
        for (TreeInfoWritable val : values) {
            if (val.getYear() < oldestYear) {
                oldestYear = val.getYear();
                oldestDistrict = val.getDistrict();
            }
        }
        result.set(oldestDistrict);
        context.write(key, result);
    }
}
