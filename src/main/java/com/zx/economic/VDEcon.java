package com.zx.economic;

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
 * @description: 输赢的总经济对比
 * @author: Ling
 * @create: 2018-12-08 13:45
 **/
public class VDEcon {

    public static class VDEconMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        private long v = 0;
        private long d = 0;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tmpData = value.toString().split(",");
            v += Integer.parseInt(tmpData[0]);
            d += Integer.parseInt(tmpData[1]);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("" + v + "," + d), NullWritable.get());
        }
    }

    public static class VDEconReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "VDEcon");
        job.setJarByClass(VDEcon.class);

        job.setMapperClass(VDEconMapper.class);
        job.setReducerClass(VDEconReducer.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path("/x/economic/part-r-00000"));

        Path outputPath = new Path("/x/VDEcon");
        FileSystem.get(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);

    }

}
