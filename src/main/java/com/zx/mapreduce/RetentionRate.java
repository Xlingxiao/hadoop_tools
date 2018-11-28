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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @program: hadoop_demo1
 * @description: 计算客户留存率 2日 3日 7日
 * @author: Ling
 * @create: 2018-11-28 09:23
 **/
public class RetentionRate {
    public static class retentionRateMapper extends Mapper<Text, Text, Text, Text> {
        private Text outputValue = new Text();

        @Override
        protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String beginTime = value.toString().split("\\s+")[2];
            String[] date = beginTime.split("T");
            String[] dates = date[0].split("-");
            outputValue.set(dates[2]);
            context.write(key, outputValue);
        }
    }
    public static class retentionRateReducer extends Reducer<Text, Text, Text, NullWritable> {

        private Text outputKey = new Text();

        private int day2User = 0;
        private int day2Total = 0;
        private int day3User = 0;
        private int day3Total = 0;
        private int day7User = 0;
        private int day7Total = 0;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context) {
            boolean firstDay = false;
            boolean secondDay = false;
            boolean thirdDay = false;
            Set<String> set = new HashSet<>();

            day7Total++;
            for (Text text : values) {
                String value = text.toString();
                set.add(value);
                if ("07".equals(value)) {
                    firstDay = true;
                } else if ("06".equals(value)) {
                    secondDay = true;
                } else if ("05".equals(value)) {
                    thirdDay = true;
                }
            }
            if (firstDay && secondDay) {
                day2User++;
            }
            if (firstDay || secondDay) {
                day2Total++;
            }
            if (firstDay && secondDay && thirdDay) {
                day3User++;
            }
            if (firstDay || secondDay || thirdDay) {
                day3Total++;
            }
            if (set.size() == 7) {
                day7User++;
            }
        }

        @Override
        protected void cleanup(Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {

            String date2Rate = String.format("%.2f", 1.0 * day2User / day2Total * 100) + "%";
            String date3Rate = String.format("%.2f", 1.0 * day3User / day3Total * 100) + "%";
            String date7Rate = String.format("%.2f", 1.0 * day3User / day7Total * 100) + "%";

            outputKey.set("6-7号 两日留存率：" + date2Rate
                    + "\n5-7号 三日留存率：" + date3Rate
                    + "\n1-7号 七日留存率：" + date7Rate);
            context.write(outputKey, NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://s01:9000");

        Job job;
        job=Job.getInstance(conf,"RetentionRate");
        job.setJarByClass(RetentionRate.class);

        // 配置此job的专属mapper和reducer
        job.setMapperClass(retentionRateMapper.class);
        job.setReducerClass(retentionRateReducer.class);
        // 配置map端输出的key和value的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // 配置输出的key和value的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        // 设置map端读取数据文件的方式
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path("/game-log"));

        Path outputPath = new Path("/x/game-output3.4");

        FileSystem.get(conf).delete(outputPath,true);
        FileOutputFormat.setOutputPath(job, outputPath);
        // 三目运算符
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
