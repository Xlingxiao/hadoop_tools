package com.zx.ecommerce;

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
 * @description: 平台的消费黄金时段
 * @author: Ling
 * @create: 2018-12-11 10:35
 **/
@SuppressWarnings("Duplicates")
public class BestTime {
    public static class BestTimeMapper extends Mapper<LongWritable, Text, Text, Text> {
        Text outputKey = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split(",");
            String tmp = data[8].substring(11, data[8].length() - 3);
            outputKey.set(tmp);
            context.write(outputKey, value);
        }
    }
//    key           value
//    2018-08-25 20 10006,诗尼曼定制家具xxxx,品质建材,2899.00,广东广州,520983902958,11991,2018-08-25 20:04,12
    public static class BestTimeReducer extends Reducer<Text, Text, Text, NullWritable> {
        Text outputKey = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            总金额
            double totalMoney = 0;
//            总订单
            long totalOrder = 0;
//            总观看次数
            long totalViewTimes = 0;
            for (Text value : values) {
                String[] data = value.toString().split(",");
                totalMoney += Double.parseDouble(data[3]);
                totalOrder++;
                totalViewTimes += Long.parseLong(data[9]);
            }
            totalMoney /= 30;
            totalOrder /= 30;
            totalViewTimes /= 30;
            String output = key.toString() + "\t" + String.format("%.2f",totalMoney) + "\t" + totalOrder + "\t" + totalViewTimes;
            outputKey.set(output);
            context.write(outputKey, NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance();
        job.setJarByClass(BestTime.class);

        job.setMapperClass(BestTimeMapper.class);
        job.setReducerClass(BestTimeReducer.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);

        job.setOutputKeyClass(TextInputFormat.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path("/newinput/ecommerce.csv"));

        Path outputPath = new Path("/x/output/BestTime");
        FileSystem.get(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);
    }
}
