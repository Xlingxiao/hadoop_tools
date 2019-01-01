package com.zx.test;

import org.apdplat.word.WordSegmenter;
import org.apdplat.word.segmentation.Word;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

/**
 * @program: hadoop_demo1
 * @description: 划分单词
 * @author: Ling
 * @create: 2018-12-12 10:15
 **/
public class WordDivide {
//    用来装入分词结果
    private static List<List<Word>> results = new LinkedList<>();

    private static String FilePath = "E:\\作业\\实训\\数据\\ecommerce.csv";
    private static String outputPath = "E:\\作业\\实训\\数据\\eco2.csv";

    public static void main(String[] args) throws IOException {
        WordDivide wd = new WordDivide();
        wd.startSegment(FilePath);
        wd.getBuffer(FilePath);
        wd.writeFile(outputPath);
    }

    /**
     * 开始分词
     * @throws FileNotFoundException 没有找到文件
     */
    private void startSegment(String filePath) throws IOException {
//        获得需要分词的源
        BufferedReader br = getBuffer(filePath);
//        开始分词
        String tmp ;
        while ((tmp = br.readLine()) != null) {
            String tmp2 = tmp.split(",")[1];
            results.add(WordSegmenter.seg(tmp2));
        }
        br.close();
    }

    /**
     * 根据文件路径获得文件输入流
     * @param fileInputPath 输入文件路径
     * @return BufferedReader
     * @throws FileNotFoundException 根据文件路径没有找到文件
     */
    private BufferedReader getBuffer(String fileInputPath) throws FileNotFoundException {
        File target = new File(fileInputPath);
        InputStreamReader is = new InputStreamReader(new FileInputStream(target));
        return new BufferedReader(is);
    }

    private void writeFile(String ouputPath) throws IOException {
        BufferedReader br = getBuffer(FilePath);
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(
                new FileOutputStream(new File(ouputPath)));
        String tmp;
        StringBuilder sb = new StringBuilder();
        int i = 0;
        while ((tmp = br.readLine()) != null) {
            sb.append(tmp).append(",").append(results.get(i++)).append("\n");
        }
        bufferedOutputStream.write(sb.toString().getBytes());
    }
}
