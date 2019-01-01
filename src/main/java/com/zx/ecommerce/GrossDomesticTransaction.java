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
 * @description: 计算国内交易总值
 * @author: Ling
 * @create: 2018-12-23 23:23
 **/
@SuppressWarnings("Duplicates")
public class GrossDomesticTransaction {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

//        根据地名分类
static final String[] PROVINCE = {"北京","天津","上海","重庆","河北",
        "山西","辽宁","吉林","黑龙江","江苏","浙江","安徽","福建","江西","山东",
        "河南","湖北","湖南","广东", "海南","四川","贵州","云南","陕西","甘肃",
        "青海","西藏","广西","内蒙古","宁夏","新疆","香港","澳门"};
        private Text outputKey = new Text();
        private Text outputValue = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split(",");
            outputValue.set(data[3]);
            for (String s : PROVINCE) {
                if (data[4].contains(s)) {
                    outputKey.set(s);
                    context.write(outputKey, outputValue);
                    return;
                }
            }
            outputKey.set("fff");
//            outputValue.set("10000000");
            context.write(outputKey, outputValue);
        }
    }


    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
        static final String[] PROVINCE = {"北京","天津","上海","重庆","河北",
                "山西","辽宁","吉林","黑龙江","江苏","浙江","安徽","福建","江西","山东",
                "河南","湖北","湖南","广东", "海南","四川","贵州","云南","陕西","甘肃",
                "青海","西藏","广西","内蒙古","宁夏","新疆","香港","澳门"};

        //        地区 交易额
        private double sum1;
        private double sum2;
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            获得国外交易额
            if (key.toString().equals("fff")) {
                for (Text value : values) {
                    sum2 += Double.valueOf(value.toString());
                }
                return;
            }
//            获得省份
            for (String s : PROVINCE)
                if (s.equals(key.toString())) outputKey.set(s);
//            每个省的总计都放到这
            double tmp = 0;
            for (Text value : values) {
                sum1 += Double.valueOf(value.toString());
                tmp += Double.valueOf(value.toString());
            }
            outputValue.set(String.valueOf(tmp));
            context.write(outputKey, outputValue);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Text outputKey = new Text();
            Text outputValue = new Text();
            outputKey.set("国内");
            outputValue.set(String.valueOf(sum1));
            context.write(outputKey,outputValue);
            outputKey.set("海外");
            outputValue.set(String.valueOf(sum2));
            context.write(outputKey,outputValue);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance();
        job.setJarByClass(GrossDomesticTransaction.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path("/newinput/ecommerce.csv"));

        Path outputPath = new Path("/x/output/GDT");
        FileSystem.get(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);
    }
}
