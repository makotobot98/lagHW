package com.lagou.mapreduce.sort;
import java.io.IOException;

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class sortMapper extends Mapper<LongWritable, Text, IntWritable, NullWritable> {

    IntWritable k = new IntWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        int num = Integer.parseInt(line.trim());
        k.set(num);
        context.write(k, NullWritable.get());
    }
}
