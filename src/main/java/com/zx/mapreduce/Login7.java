package com.zx.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
 * @description: 根据用户的权限值，判断此用户在哪些天登录过
 * @author: Ling
 * @create: 2018-11-30 10:39
 **/
public class Login7 {

    private static String userID = null;

    public static class login7Mapper extends Mapper<Text, Text, Text, IntWritable> {
//        fff3bba4-d762-49fd-916f-e9f3417c0a0e	126
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            if (key.toString().equals(userID)) {
                context.write(key,new IntWritable(Integer.parseInt(value.toString())));
            }
        }
    }

    public static class login7Reduce extends Reducer<Text, IntWritable, Text, Text> {
        //        fff3bba4-d762-49fd-916f-e9f3417c0a0e	126

        private Text outputValue = new Text();
        private Text outputKey = new Text();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//            如果用户输入的id不存在
            if (!(key.toString().length() <= 0)) {
                context.write(new Text(userID), new Text("无查询结果"));
                return;
            }
            StringBuilder sb = new StringBuilder();
//            这里values 中 最多只有一个 value
            for (IntWritable value : values) {
//                1111110
                int flag = value.get();
                for (int i = 0; i < 7; i++) {
                    int tmp = 1 << i;
                    if ((tmp & flag) != 0) {
                        sb.append(i).append("\t");
                    }
                }
            }
            outputValue.set(sb.toString());
            outputKey.set(key.toString());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(outputKey,outputValue);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        userID = args[0];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Authority-zx");
        job.setJarByClass(Authority.class);

        job.setMapperClass(login7Mapper.class);
        job.setReducerClass(login7Reduce.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("/x/game-authority/part-r-00000"));
        Path outputPath = new Path("/x/game-login7");
        FileSystem.get(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);

    }

}
