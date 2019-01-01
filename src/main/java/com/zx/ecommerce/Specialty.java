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
import java.util.*;
import java.util.stream.Collectors;

/**
 * @program: hadoop_demo1
 * @description: 获得每个省销售最好的分类
 * @author: Ling
 * @create: 2018-12-12 13:24
 **/
@SuppressWarnings("Duplicates")
public class Specialty {
    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        Text outputKey = new Text();
        Text outputValue = new Text();
        static final String[] PROVINCE = {"北京","天津","上海","重庆","河北",
                "山西","辽宁","吉林","黑龙江","江苏","浙江","安徽","福建","江西","山东",
                "河南","湖北","湖南","广东", "海南","四川","贵州","云南","陕西","甘肃",
                "青海","西藏","广西","内蒙古","宁夏","新疆","香港","澳门"};
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split(",", 10);
            String province = data[4].substring(0, 2);
            boolean flag = false;
            for (String s : PROVINCE) {
                if (province.contains(s)) {
                    province = s;
                    flag = true;
                    break;
                } else {
                    continue;
                }
            }
            if (!flag) {
                province = "海外";
            }
            outputKey.set(province);
            outputValue.set(data[9].substring(1, data[9].length() - 1));
            context.write(outputKey,outputValue);
        }
    }
//    上海  房屋, 卫士, 自流平, 美, 缝, 剂, 瓷砖, 地砖, 专用, 双, 组份, 真, 瓷, 胶, 防水, 填缝剂, 镏, 金色
    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
        Text outputValue = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            Map<String,Integer> provinceSpeciality = new HashMap();
    //            将所有标签统计频率
            for (Text value : values) {
                String[] data = value.toString().split(",");
                for (String datum : data) {
                    if (provinceSpeciality.containsKey(datum)) {
                        provinceSpeciality.put(datum, provinceSpeciality.get(datum) + 1);
                    }else
                        provinceSpeciality.put(datum, 1);
                }
            }
    //            map按照value排序
            Map<String, Integer> result = provinceSpeciality.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                            (oldValue, newValue) -> oldValue, LinkedHashMap::new));
            int i = 20;
            for (Map.Entry<String, Integer> entry : result.entrySet()) {
                if (i-- > 0) {
                    sb.append(entry.getKey()).append(",");
                }else
                    break;
            }
//            for (String s : result.keySet()) {
//                if (i-- > 0) {
//                    sb.append(s).append(",");
//                }else
//                    break;
//            }
            outputValue.set(sb.toString());
            context.write(key,outputValue);
        }
}

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance();
        job.setJarByClass(Specialty.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("/newinput/eco2.csv"));

        Path outputPath = new Path("/x/output/specialty");
        FileSystem.get(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);
    }

}
