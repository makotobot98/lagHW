package com.lagou.mapreduce.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class sortReducer extends Reducer<IntWritable, NullWritable, IntWritable, IntWritable> {

    static int rank = 1;
    IntWritable k = new IntWritable();
    @Override
    protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for (NullWritable n : values) {
            k.set(rank++);
            context.write(k, key);
        }

    }
}
