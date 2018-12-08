//package com.zx.mapreduce.common;
//
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//
//import java.io.IOException;
//
///**
// * @program: hadoop_demo1
// * @description: 划分输赢
// * @author: Ling
// * @create: 2018-12-08 14:45
// **/
//public class DevideVOD {
//    public static class VDEconMapper extends Mapper<LongWritable, Text, Text, Text> {
//        @Override
//        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            context.write(new Text(value.toString().split("\\s+")[8]), value);
//        }
//    }
//
//    public static class VDEconReducer extends Reducer<Text, Text, Text, Text> {
//        @Override
//        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            StringBuilder Vsb = new StringBuilder();
//            StringBuilder Dsb = new StringBuilder();
//            for (Text value : values) {
//                String[] singularData = value.toString().split("\\s+");
//                if (singularData[1].equals("1")) {
//                    Vsb.append("1").append(",");
//                }
//            }
//        }
//    }
//}
