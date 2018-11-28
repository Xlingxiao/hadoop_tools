package com.zx.mapreduce;

import java.io.IOException;

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


/**
 * @Title: NewOldUser.java
 * @Description:
 * 	计算前两天的新老用户
 *
 * @Company: kkb-xh
 * @author ljw
 * @date 2018-11-27 11:06
 * @version 1.0   */
public class NewOldUser {

    public static class newOldUserMapper extends Mapper<Text, Text, Text, Text>{

        private Text outputValue = new Text();

        //cd8acdd8-0fca-49cd-8ec1-ecc63a626511	Android	4.4	2017-01-01T14:46:08	2017-01-01T23:42:13	32165
        @Override
        protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {

            String beginTime = value.toString().split("\\s+")[2];
            String[] date = beginTime.split("T");
            String[] dates = date[0].split("-");
            outputValue.set(dates[2]);
            context.write(key, outputValue);
        }
    }

    // <cd8acdd8-0fca-49cd-8ec1-ecc63a626511,01>
    // <cd8acdd8-0fca-49cd-8ec1-ecc63a626511,02>
    // <cd8acdd8-0fca-49cd-8ec1-ecc63a626511,01  02>
    public static class newOldUserReducer extends Reducer<Text, Text, Text, NullWritable>{

        private Text outputKey = new Text();
        private int newUser = 0;
        private int oldUser = 0;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
                throws IOException, InterruptedException {

            boolean firstDay = false;
            boolean secondDay = false;

            for (Text value : values) {

                String valueString = value.toString();
                if ("01".equals(valueString)) {
                    firstDay = true;
                }else if ("02".equals(valueString)) {
                    secondDay = true;
                }
            }

            if (firstDay && secondDay) {
                oldUser++;
            }else if (!firstDay && secondDay) {
                newUser++;
            }
        }

        @Override
        protected void cleanup(Reducer<Text, Text, Text, NullWritable>.Context context)
                throws IOException, InterruptedException {

            outputKey.set("前两天的新用户："+newUser+"\t"+"老用户："+oldUser);
            context.write(outputKey, NullWritable.get());
        }
    }

    public static void main(String[] args) {


        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://s01:9000");

            Job job;
            job=Job.getInstance(conf,"NewOldUser");
            job.setJarByClass(NewOldUser.class);

            // 配置此job的专属mapper和reducer
            job.setMapperClass(newOldUserMapper.class);
            job.setReducerClass(newOldUserReducer.class);
            // 配置map端输出的key和value的数据类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            // 配置输出的key和value的数据类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            // 设置map端读取数据文件的方式
            job.setInputFormatClass(KeyValueTextInputFormat.class);

            	FileInputFormat.addInputPath(job, new Path("/game-log"));
//            Path inputPath1 = new Path("/game-log/3.2/part-r-00000");
//            Path inputPath2 = new Path("/game-log/3.2/part-r-00001");
//            FileInputFormat.addInputPath(job, inputPath1);
//            FileInputFormat.addInputPath(job, inputPath2);

            Path outputPath = new Path("/x/game-output3.3");

            FileSystem.get(conf).delete(outputPath,true);
            FileOutputFormat.setOutputPath(job, outputPath);
            // 三目运算符
            System.exit(job.waitForCompletion(true)?0:1);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
