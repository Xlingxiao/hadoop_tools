package com.zx.ecommerce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @program: hadoop_demo1
 * @description: 追寻同类商品的货源地址 标题名字不同，但是属于相同商品的宝贝
 * 它们的价格分布情况 求五分位数
 * 判断相同商品的产地 求它们的比例
 *
 * 属于同一类型 且 标签相同率超过70%判断为相同商品
 *
 * 输出格式：宝贝名称 产地TOP1 TOP1占比 价格区间：均值 五分位数
 * @author: Ling
 * @create: 2018-12-11 11:58
 **/
@SuppressWarnings("Duplicates")
public class FindFake {
//    //    将类型作为key，其他的作为value
//    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
//        Text outputKey = new Text();
//        @Override
//        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            String[] data = value.toString().split(",", 10);
//            outputKey.set(data[4]);
//            context.write(outputKey, value);
//        }
//    }

//    map阶段使用Specialty 的mapper

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
//        卖家是A点 却在卖 B点的商品的数量 以及卖的是啥 冒充什么地方
        static final String[] PROVINCE = {"北京","天津","上海","重庆","河北",
                "山西","辽宁","吉林","黑龙江","江苏","浙江","安徽","福建","江西","山东",
                "河南","湖北","湖南","广东", "海南","四川","贵州","云南","陕西","甘肃",
                "青海","西藏","广西","内蒙古","宁夏","新疆","香港","澳门"};

        Text outputV = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int countSelf = 0;
            int countOther = 0;
            Map<String, Integer> provinceMap = new HashMap<>();
            for (Text value : values) {
                countSelf++;
                String[] data = value.toString().split(",");
                for (String s : PROVINCE) {
                    for (String datum : data) {
//                        如果标签中包含了地名
                        if (datum.contains(s)) {
//                            包含的地名不是本地地名
                            if (!datum.contains(key.toString())) {
                                countOther++;
//                                获得冒充的省份
                                if (provinceMap.containsKey(datum)) {
                                    provinceMap.put(datum, provinceMap.get(datum) + 1);
                                }else
                                    provinceMap.put(datum, 1);
                            }
                        }
                    }
                }
            }
            Map<String, Integer> result = provinceMap.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                            (oldValue, newValue) -> oldValue, LinkedHashMap::new));
            String tmp ="";
            for (String s : result.keySet()) {
                tmp = s;
                break;
            }
            outputV.set(countOther + "\t" + tmp);
            context.write(key,outputV);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance();
        job.setJarByClass(FindFake.class);

        job.setMapperClass(Specialty.MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("/newinput/eco2.csv"));

        Path outputPath = new Path("/x/output/findfake");
        FileSystem.get(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);
    }
}
