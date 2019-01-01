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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @program: hadoop_demo1
 * @description: 计算真正的复购率
 * @author: Ling
 * @create: 2018-12-26 15:14
 **/
@SuppressWarnings("Duplicates")
public class Repurchase2 {
    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        Text outputKey = new Text();
        Text outputValue = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\t", 2);
            outputKey.set(data[0]);
            outputValue.set(data[1]);
            context.write(outputKey, outputValue);
        }
    }

    @SuppressWarnings("Duplicates")
    public static class MyReducer extends Reducer<Text, Text, Text, NullWritable> {
        String[] KINDS = new String[]{"休闲家具", "内衣会场", "医药健康", "品质建材", "图书乐器会场",
                "大家电会场", "女装会场", "女装商场同款", "女装风格好店", "女鞋会场", "家纺家饰会场",
                "小家电会场", "手机会场", "手表眼镜", "数码家电会场", "母婴主会场", "洗护清洁会场",
                "潮酷数码会场", "灯具灯饰会场", "珠宝饰品", "生鲜会场", "电脑办公会场", "男装会场",
                "男装风格好店", "百货会场", "童装会场", "箱包配饰", "精品家具", "美妆主会场",
                "车品配件", "运动户外", "进口尖货", "零食会场", "食品主会场", "高端洗护会场"};

        private class Kind {
            String name;
            long countUser;
            long more;

            Kind(String name) {
                this.name = name;
                this.countUser = 0;
                this.more = 0;
            }
        }

        List<Kind> list = new ArrayList<>();

        @Override
        protected void setup(Context context){
            for (String kind : KINDS) {
                list.add(new Kind(kind));
            }
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context){
            for (Text value : values) {
                List<String> tmpList = new LinkedList<>();
                String[] data = value.toString().split("\t");
                Kind tmp ;
                for (String datum : data) {
                    tmp = getKind(datum);
                    if (tmp == null) continue;
                    tmp.countUser++;
                    if (tmpList.contains(datum)) continue;
                    if (judge(data, datum)) {
                        tmp.more++;
                        tmpList.add(datum);
                    }
                }
            }
        }

        /*判断列表中是否有超过两个相同标签的*/
        private boolean judge(String[] data, String target) {
            int i = 0;
            for (String datum : data)
                if (datum.equals(target)) {
                    i++;
                }
            return i > 1;
        }

        //        根据名字返回种类
        private Kind getKind(String name) {
            for (Kind kind : list) {
                if (kind.name.equals(name)) {
                    return kind;
                }
            }
            return null;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Text outputKey = new Text();
            for (Kind kind : list) {
                double tmp = kind.more * 1.0 / kind.countUser * 100;
                String sb = kind.name + "\t" + String.format("%.2f", tmp);
                outputKey.set(sb);
                context.write(outputKey, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance();
        job.setJarByClass(Repurchase2.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path("/x/output/Repurchase1/part-r-00000"));

        Path outputPath = new Path("/x/output/Repurchase2");
        FileSystem.get(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);
    }

}
