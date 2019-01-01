package com.zx.ecommerce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
 * @description: 计算复购率:每个用户购买相同种类的商品的复购率
 * 复购率高的商品种类我们做活动的时候折扣力度就可以稍微低一点，
 * 复购率低的商品可以折扣稍微高一点，
 * 因为复购率高的商品用户会经常购买现阶段需要这类商品的用户量也会比较大，只需要有一点点折扣用户就会促进用户进行购买
 * 复购率较低的商品目前需要的用户量可能并不是很高，就需要略微大一点的折扣或者浮夸一点的活动让那些暂时并不需要的用户
 * 也来进行购买
 * 复购率的计算：购买过该商品两次以上的人/购买过该商品的人
 * 计算思路：mapper：user_id 【商品种类集合】
 *          reducer：每次调用reduce函数就说明购买过的总人数+1
 *          在reduce函数中可以计算每个种类的商品被该用户购买了多少次： 10001 品质建材*2 女装会场*2
 *          cleanup函数中
 * @author: Ling
 * @create: 2018-12-26 13:12
 **/
public class Repurchase {
    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        Text outputKey = new Text();
        Text outputValue = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split(",");
            String kind = data[2];
            String userId = data[7];
            outputKey.set(userId);
            outputValue.set(kind);
            context.write(outputKey,outputValue);
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
        Text outputValue = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text value : values) {
                sb.append(value.toString()).append("\t");
            }
            outputValue.set(sb.toString());
            context.write(key, outputValue);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance();
        job.setJarByClass(Repurchase.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("/newinput/ecommerce.csv"));

        Path outputPath = new Path("/x/output/Repurchase1");
        FileSystem.get(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);
    }
}
