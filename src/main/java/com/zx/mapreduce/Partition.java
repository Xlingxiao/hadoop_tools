package com.zx.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @program: hadoop_demo1
 * @description: 对输出进行分区
 * @author: Ling
 * @create: 2018-11-27 09:24
 **/
public class Partition {
    public static class partitionMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        protected void map(LongWritable key, Text value,
                           Mapper<LongWritable, Text, Text, NullWritable>.Context context)
                throws IOException, InterruptedException {
            context.write(value, NullWritable.get());
        }
    }
    public static class partitionReducer extends Reducer<Text, NullWritable, Text, NullPointerException> {
        protected void reduce(Text key, Iterable<NullWritable> values,
                              Reducer<Text, NullWritable, Text, NullWritable>.Context context)
                throws IOException, InterruptedException {
            for (NullWritable value : values) {
                context.write(key, NullWritable.get());
            }
        }
    }

    public static class partitionPart extends Partitioner<Text, NullWritable> {

        @Override
        public int getPartition(Text key, NullWritable nullWritable, int i) {
            String beginTime = key.toString().split("\\s+")[3];
            String date = beginTime.split("T")[0];
            String[] dates = date.split("-");
            for (int j = 0; j < 7; j++) {
                if (dates[2].equals("0" + j)) {
                    return (j - 1) % i;
                }
            }
            return 0 % i;
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        获得配置文件对象
        Configuration conf = new Configuration();
        conf.set("defaultFS", "hdfs://master:9000");
//        创建Job
        Job job = Job.getInstance(conf, "partition");
        job.setJarByClass(Partition.class);
//        获得输入流
        FileInputFormat.addInputPath(job, new Path("/game-log/游戏日志分析-2017-01-01-2017-01-07.log"));
//        配置map处理逻辑
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(partitionMapper.class);
//        配置分区
        job.setNumReduceTasks(7);
        job.setPartitionerClass(partitionPart.class);

//        配置输出流
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        Path outputPath = new Path("/x/game-output");
        FileOutputFormat.setOutputPath(job,outputPath);
        FileSystem.get(conf).delete(outputPath, true);
//        开始执行
        job.waitForCompletion(true);
    }
}
