package com.zx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
 * @description:
 * @author: Ling
 * @create: 2018-11-23 11:40
 **/
public class WordCount {
    /**
     *  Mapper中的泛型 前两个是输入的 key的类型-value的类型
     *  后两个是输出的 key的类型-value的类型
     *  这里输入的key为long，指的是每个单词的位置
     *  输出的key是每一个的单词，value是每个单词出现的次数
     */
    public static class wordcount1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

//        创建一个int 值为 1
        private final static IntWritable one = new IntWritable(1);
//        创建一个输出的key,类型在之前就设置过了
        private Text outputKey = new Text();

        /**
         *
         * @param key 每个单词的位置索引
         * @param value 每个单词
         * @param context hadoop中进行内容交换的区域
         * @throws IOException 向context中写内容时可能出现io异常
         * @throws InterruptedException 向context中写内容时可能出现Interrupted异常
         */
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {

            // \\s+:一个或者多个空格的统称
            String[] words = value.toString().split("\\s+");
            for (String word : words) {
                outputKey.set(word);
                context.write(outputKey, one);
            }
        }
    }

    /**
     * Reduce 部分，将map部分分散的内容进行聚合，得到结果
     */
    public static class wordcount1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

//        每个单词出现的总的次数
        private IntWritable result = new IntWritable();

        // <aa,1>  <bb,1>  <cc,1> <aa,1>  <bb,1> <aa,1>
        // <aa,1 1 1> <bb,1 1> <cc,1>

        /**
         *
         * @param key 为上文map输出的key
         * @param values map输出的value
         * @param context 程序中用于交换数据的对象
         * @throws IOException context 可能出现的异常
         * @throws InterruptedException context 可能出现的异常
         */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            /**
             * 根据我的猜测调用每个reduce函数的时候是根据key来进行调用的，
             * 相同的key值共享一个迭代类型的int数据
             * reduce函数运行一次就是在处理一个key值
             * hadoop在这个过程中应该是自动的为我们做了将相同key值的数据进行一个处理这样的操作
             */

//            每个单词出现的总的次数
            int sum = 0;
//            将迭代类型中所有的值都相加
            for (IntWritable value : values) {
                sum += value.get();
            }
//            将结果放到result中
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) {

        try {
//            创建配置文件
            Configuration conf = new Configuration();
//            conf.set("fs.defaultFS", "hdfs://s01:9000");

            Job job;
            job=Job.getInstance(conf,"wordcount2");
            job.setJarByClass(WordCount.class);

            // 配置此job的专属mapper和reducer
            job.setMapperClass(wordcount1Mapper.class);
            job.setReducerClass(wordcount1Reducer.class);
            // 配置输出的key和value的数据类型
            job.setOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            // 设置map端读取数据文件的方式
            job.setInputFormatClass(TextInputFormat.class);

            FileInputFormat.addInputPath(job, new Path("/x/input"));
            Path outputPath = new Path("/x/output4");

            FileSystem.get(conf).delete(outputPath,true);
            FileOutputFormat.setOutputPath(job, outputPath);
            // 三目运算符
            System.exit(job.waitForCompletion(true)?0:1);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
