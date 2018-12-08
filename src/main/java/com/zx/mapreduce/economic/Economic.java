package com.zx.mapreduce.economic;

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
 * @description: 4.	游戏的胜利与团队的整体经济（补兵，击杀，助攻）相关。
 * @author: Ling
 * @create: 2018-12-07 09:33
 **/
public class Economic {
    public static class EconomicMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            outputKey.set(value.toString().substring());
            String[] data = value.toString().split("\\s+");
//            输出的格式为key:场次，value：胜/负，补兵
            context.write(new Text(data[8]), new Text(data[1] + " " + data[2]));
        }

    }

    public static class EconomicReducer extends Reducer<Text, Text, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int v_sum = 0;
            int d_sum = 0;
            for (Text value : values) {
                String[] data = value.toString().split("\\s+");
                if (data[0].equals("1")) {
                    v_sum += Integer.valueOf(data[1]);
                } else {
                    d_sum += Integer.valueOf(data[1]);
                }
            }
            context.write(new Text("" + v_sum + "," + d_sum), NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance();
        job.setJarByClass(Economic.class);

        job.setMapperClass(EconomicMapper.class);
        job.setReducerClass(EconomicReducer.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);

        job.setOutputKeyClass(TextInputFormat.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path("/x/modify/part-r-00000"));

        Path outputPath = new Path("/x/economic");
        FileSystem.get(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);
    }

}
