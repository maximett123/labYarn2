package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxTreeCountReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
    private Text result = new Text();

    public void reduce(NullWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        int maxCount = 0;
        String maxDistrict = "";
        for (Text val : values) {
            String[] parts = val.toString().split("\t");
            int count = Integer.parseInt(parts[1]);
            if (count > maxCount) {
                maxCount = count;
                maxDistrict = parts[0];
            }
        }
        result.set(maxDistrict + "\t" + maxCount);
        context.write(NullWritable.get(), result);
    }
}
