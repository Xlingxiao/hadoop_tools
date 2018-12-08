package com.zx.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @program: hadoop_demo1
 * @description: 将用户的每日登陆计算为一个数值，通过二进制可以判断用户什么时候登陆过
 * for instance ： 数值 22 = 00010100 这个就可以表示用户在第 3 和第 5 天登陆过
 * @author: Ling
 * @create: 2018-11-30 10:11
 **/
public class Authority {

    public static class authorityMapper extends Mapper<Text, Text, Text, IntWritable> {
//        c9ae3e30-521f-4816-8439-17fcd78132ca	iOS	11.2	2017-01-07T23:59:55	2017-01-08T00:00:01	6
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            IntWritable day = new IntWritable(
                    Integer.parseInt(
                            value.toString().split("\\s+")[3].substring(8, 10)));
            context.write(key, day);
        }
    }

    public static class authorityReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        //        c9ae3e30-521f-4816-8439-17fcd78132ca 1 3 5
        IntWritable outputValue = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int flag = 0;
            for (IntWritable value : values) {
                flag |= 1 << (value.get() - 1);
            }
            outputValue.set(flag);
            context.write(key, outputValue);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Authority-zx");
        job.setJarByClass(Authority.class);

        job.setMapperClass(authorityMapper.class);
        job.setReducerClass(authorityReducer.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("/game-log"));
        Path outputPath = new Path("/x/game-authority");
        FileSystem.get(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);

    }


}
