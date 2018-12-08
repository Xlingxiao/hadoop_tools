package com.zx.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @program: hadoop_demo1
 * @description: 真正的Top20
 * @author: Ling
 * @create: 2018-11-29 09:50
 **/
public class Top20 {

    /**
     * Map阶段
     */
    public static class TopKMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, NullWritable.get());
        }
    }


//    这个类的意思是mapreduce中进行排序比较的类，所有的排序需要的比较都会用到这个类中的方法
    public static class TopKComparator extends WritableComparator {
        public TopKComparator() {
            super(Text.class, true);
        }

    //        ffee437d-98b4-413f-9621-af1fa667cba3	首次登陆时间：2017-01-07T07:53:38	总在线时间：433961	总在线次数:18
//        fff166b9-50d3-4175-b25a-251a994b3d7c	首次登陆时间：2017-01-04T05:37:22	总在线时间：234431	总在线次数:11
//        先以上线总时间排序，再以上线次数排序
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            String[] A = a.toString().split("\\s+");
            String[] B = b.toString().split("\\s+");

//        获得总时间
            Integer atime = Integer.valueOf(A[2]);
            Integer btime = Integer.valueOf(B[2]);
//        获得总次数
            Integer atimes = Integer.valueOf(A[3]);
            Integer btimes = Integer.valueOf(B[3]);

            return atime.compareTo(btime) == 0 ? atimes.compareTo(btimes) == 0 ? A[1].compareTo(B[1])
                    : btimes.compareTo(atimes) : btime.compareTo(atime);
        }
    }

    public static class TopKReduce extends Reducer<Text, NullWritable, Text, NullWritable> {
        private int num = 20;

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            if (num-- > 0) {
                context.write(key, NullWritable.get());
            }
        }
    }




    /**
     * 主函数：用于配置、调度任务执行
     * @param args 不需要传入
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        获得job和配置
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TopK");
        job.setJarByClass(Top20.class);
//        配置mapper和reducer
        job.setMapperClass(TopKMapper.class);
        job.setReducerClass(TopKReduce.class);
//        配置mapper的输入
        job.setInputFormatClass(TextInputFormat.class);
//        配置mapper的输出
//        job.setMapOutputValueClass(NullWritable.class);
//        job.setMapOutputKeyClass(Text.class);
//        配置reducer的输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
//        配置排序规则
        job.setSortComparatorClass(TopKComparator.class);
        job.setGroupingComparatorClass(TopKComparator.class);
//        配置io路径
        FileInputFormat.addInputPath(job,  new Path("/x/game-Top20Info/part-r-00000"));
        Path outputPath = new Path("/x/game-Top20");
        FileSystem.get(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);
//        运行
        job.waitForCompletion(true);

    }
}
