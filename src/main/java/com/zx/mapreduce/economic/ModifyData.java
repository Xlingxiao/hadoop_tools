package com.zx.mapreduce.economic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
 * @description: 修改原始数据
 * @author: Ling
 * @create: 2018-12-07 11:26
 **/
public class ModifyData {
    public static class ModifyMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }

    }

    public static class ModifyReducer extends Reducer<LongWritable, Text, Text, NullWritable> {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text value : values) {
                String[] data = value.toString().split("\\s+");
//                修改助攻数量
                int seven = Integer.valueOf(data[7]);
                seven = seven > 35 ? seven > 65 ? seven > 85 ? (int) (seven - seven * 0.8)
                        : (int) (seven - seven * 0.6)
                        : (int) (seven - seven * 0.5)
                        : seven;
                data[7] = String.valueOf(seven);
//                修改胜方补兵数量
                if (data[1].equals("" + 1)) {
                    int times = Integer.parseInt(data[2]);
                    times = times + times * 3 / 10;
                    data[2] = "" + times;
                }

                for (String datum : data) {
                    sb.append(datum).append(" ");
                }
            }
            context.write(new Text(sb.toString()), NullWritable.get());

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance();
        job.setJarByClass(ModifyData.class);

        job.setMapperClass(ModifyMapper.class);
        job.setReducerClass(ModifyReducer.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(TextInputFormat.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path("/input/英雄详细信息.txt"));

        Path outputPath = new Path("/x/modify");
        FileSystem.get(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);
    }
}
