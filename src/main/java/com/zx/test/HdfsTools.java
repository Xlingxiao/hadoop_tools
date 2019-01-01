package com.zx.test;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class HdfsTools {

    FileSystem fs = null;

    public void init_config() throws IOException {
        System.setProperty("HADOOP_USER_NAME","hadoop");
        //  1.获取集群的配置信息
        Configuration conf = new Configuration();
        //  2.获取集群文件系统
        fs = FileSystem.get(conf);
    }

    public static void main(String[] args) throws IOException {
        HdfsTools tools = new HdfsTools();
        tools.init_config();
//        tools.createDir("/testTools");
//        tools.createFile("/testTools/
//        tools.test(new Path("/x/output"));
        tools.printFileContent("/x/output/BestTime/part-r-00000");
        tools.closeHdfs();
    }

    /**
     * 获取文件内容
     * @param filePath 文件路径（HDFS）
     * @throws IOException hdfs中找不到文件
     */
    void printFileContent(String filePath) throws IOException {
        Path path = new Path(filePath);
        FSDataInputStream FSin = fs.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(FSin));
        String tmp ;
        StringBuilder sb = new StringBuilder();
        while ((tmp = br.readLine()) != null) {
            sb.append(tmp).append("\n");
        }
        FSin.close();
        System.out.println(sb.toString());
    }

    /**
     * 创建文件
     * @param fileName 需要创建文件的路径
     * @throws IOException 连接出错
     */
    void createFile(String fileName) throws IOException {
        Path path =  new Path(fileName);
        fs.create(path);
        System.out.println("文件创建成功");
    }

    /**
     * 创建文件夹
     * @param DirName 创建文件夹的全路径
     * @throws IOException 连接出错
     */
    void createDir(String DirName) throws IOException {
        Path path =  new Path(DirName);
        fs.mkdirs(path);
        System.out.println("文件夹创建成功");
    }

    /**
     * 遍历文件夹下所有文件
     * @param dirPath 目标文件
     * @throws IOException 异常
     */
    void test(Path dirPath) throws IOException {
        FileStatus[] status = fs.listStatus(dirPath);
        for (int i = 0; i < status.length; i++) {
            System.out.println(status[i].getPath());
            if (status[i].isDirectory()) {
                test(status[i].getPath());
            }
        }
    }


//    关闭连接
    void closeHdfs() throws IOException {
        fs.close();
    }
}
