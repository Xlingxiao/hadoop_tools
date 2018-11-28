package com.zx.mapreduce;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @program: hadoop_demo1
 * @description:
 * @author: Ling
 * @create: 2018-11-28 11:04
 **/
public class Top20Info {
    public static class Top20InfoMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class Top20InfoReducer extends Mapper<Text, Text, Text, NullWritable> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            super.map(key, value, context);
        }
    }

}
