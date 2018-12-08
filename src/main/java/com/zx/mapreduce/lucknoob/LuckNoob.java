package com.zx.mapreduce.lucknoob;

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
import java.util.*;

/**
 * @program: hadoop_demo1
 * @description: 计算最佳躺赢英雄top5
 * @author: Ling
 * @create: 2018-12-08 14:42
 **/
public class LuckNoob {

    public static class NoobMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\\s+");
            Text outputValue = new Text();
//            将kda计算出来放到data5中
            float tmp = data[6].equals("0") ? (Float.valueOf(data[5]) + Float.valueOf(data[7]))
                    : (Float.valueOf(data[5]) + Float.valueOf(data[7])) / Float.valueOf(data[6]);
            data[5] = String.valueOf(tmp);
//            便利data 0 - 5;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 6; i++) {
                sb.append(data[i]).append(",");
            }
            outputValue.set(sb.toString());
            context.write(new Text(data[0]), outputValue);
        }
    }

    public static class NoobReducer extends Reducer<Text, Text, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] sites = new String[]{"Up", "Ranger", "Mid", "Help", "Down"};
//            所有赢的位置统计
            int countSite[] = new int[5];
//            所有赢的kda 用于计算五分位值
            List<Float> KDACount = new LinkedList<>();
            for (Text value : values) {
                String[] data = value.toString().split(",");
                if (!data[1].equals("1")) {
                    continue;
                }
                for (int i = 0; i < 5; i++) {
//                    统计英雄的位置
                    if (data[3].equals(sites[i])) {
                        countSite[i]++;
                        break;
                    }
                }
//                取出kda
                KDACount.add(Float.valueOf(data[5]));
            }
//            获得最佳位置
            int goodSite = 0;
            for (int i = 0; i < countSite.length; i++) {
                goodSite = countSite[i] > countSite[goodSite] ? i : goodSite;
            }
//            获得kda的五分位值
            Collections.sort(KDACount);
            float[] fiveValue = new float[5];
            fiveValue[0] = KDACount.get(0);
            fiveValue[1] = KDACount.get((int) (KDACount.size() * 0.25));
            fiveValue[2] = KDACount.get((int) (KDACount.size() * 0.5));
            fiveValue[3] = KDACount.get((int) (KDACount.size() * 0.75));
            fiveValue[4] = KDACount.get(KDACount.size() - 1);
//            获得kda 平均数
            float sum = 0;
            for (Float integer : KDACount) {
                sum += integer;
            }
            String avg =String.valueOf(sum / KDACount.size());
            Text outputKey = new Text();
            StringBuilder sb = new StringBuilder();
            String divideChar = "\t";
            sb.append(key).append(divideChar);
            sb.append(sites[goodSite]).append(divideChar);
            sb.append(avg).append(divideChar);
            for (float v : fiveValue) {
                sb.append(String.valueOf(v));
                sb.append(divideChar);
            }
            outputKey.set(sb.toString());
            context.write(outputKey, NullWritable.get());
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance();
        job.setJarByClass(LuckNoob.class);

        job.setMapperClass(NoobMapper.class);
        job.setReducerClass(NoobReducer.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);

        job.setOutputKeyClass(TextInputFormat.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path("/x/modify/part-r-00000"));

        Path outputPath = new Path("/x/LuckNoob");
        FileSystem.get(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);
    }
}
