//package com.zx.transition;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FSDataInputStream;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//
//import java.net.URI;
//
///**
// * @program: hadoop_demo1
// * @description:
// * @author: Ling
// * @create: 2018-12-30 21:45
// **/
//public class BestTimeToRedis {
//    public static void main(String[] args) {
//        FileSystem fs = null;
//        try {
//            Configuration conf = new Configuration();// 加载配置文件
//
//            fs = FileSystem.get(conf); // 创建文件系统实例对象
//
//            Path p= new Path("/x/output/BestTime/part-r-00000"); // 默认是读取/user/navy/下的指定文件
//
//
//            System.out.println("要查看的文件路径为："+fs.getFileStatus(p).getPath());
//
//            FSDataInputStream fsin = fs.open(fs.getFileStatus(p).getPath());
//            byte[] bs = new byte[1024 * 1024];
//            int len = 0;
//            while((len = fsin.read(bs)) != -1){
//                System.out.print(new String(bs, 0, len));
//            }
//
//            System.out.println();
//            fsin.close();
//        } catch (Exception e) {
//            System.out.println("hdfs操作失败!!!");
//        }
//    }
//
//}
