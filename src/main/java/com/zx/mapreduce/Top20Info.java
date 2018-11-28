package com.zx.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @program: hadoop_demo1
 * @description: 单次登录时间最长top10
 * @author: Ling
 * @create: 2018-11-28 11:04
 **/
public class Top20Info {
    public static class Top20InfoMapper extends Mapper<Text, Text, Text, Text> {
//        c9ae3e30-521f-4816-8439-17fcd78132ca	iOS	11.2	2017-01-07T23:59:55	2017-01-08T00:00:01	6
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class Top20InfoReducer extends Reducer<Text, Text, Text, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            super.reduce(key, values, context);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        1.获得job和Configuration
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TOP20");
        job.setJarByClass(Top20Info.class);
//        2.指定job的mapper和reducer
        job.setMapperClass(Top20InfoMapper.class);
        job.setReducerClass(Top20InfoReducer.class);
//        3.设置mapper的输入文件方式
//        这里为什么要设置为KeyValueText类型的我目前不清楚
        job.setInputFormatClass(KeyValueTextInputFormat.class);
//        4.设置mapper的输出类型 ---reducer 的输入类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
//        5.设置job的输出类型 -- reducer的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
//        6.设置输入文件路径
        FileInputFormat.addInputPath(job, new Path("/game-log"));
//        7.设置文件输出路径
//        如果不确定输出路径是否存在的话可以将输出路径删除
        Path outputPath = new Path("/x/game-test");
        FileSystem.get(conf).delete(outputPath,true);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);



    }

}
