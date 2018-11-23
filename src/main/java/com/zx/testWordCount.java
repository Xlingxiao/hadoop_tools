package com.zx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @program: hadoop_demo1
 * @description:
 * @author: Ling
 * @create: 2018-11-23 11:40
 **/
public class testWordCount {
    public static class wordcount1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text outputKey = new Text();

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {

            // \\s+:一个或者多个空格的统称
            String[] words = value.toString().split("\\s+");
            for (String word : words) {
                outputKey.set(word);
                context.write(outputKey, one);
            }
        }
    }

    public static class wordcount1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        // <aa,1>  <bb,1>  <cc,1> <aa,1>  <bb,1> <aa,1>
        // <aa,1 1 1> <bb,1 1> <cc,1>
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable value : values) {
                sum+=value.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) {

        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://self:9000");

            Job job;
            job=Job.getInstance(conf,"wordcount1");
            job.setJarByClass(testWordCount.class);

            // 配置此job的专属mapper和reducer
            job.setMapperClass(wordcount1Mapper.class);
            job.setReducerClass(wordcount1Reducer.class);
            // 配置输出的key和value的数据类型
            job.setOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            // 设置map端读取数据文件的方式
            job.setInputFormatClass(TextInputFormat.class);

            FileInputFormat.addInputPath(job, new Path("/x/input/test.txt"));
            Path outputPath = new Path("/x/output1");

            FileSystem.get(conf).delete(outputPath,true);
            FileOutputFormat.setOutputPath(job, outputPath);
            // 三目运算符
            System.exit(job.waitForCompletion(true)?0:1);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
