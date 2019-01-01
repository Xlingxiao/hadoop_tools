package com.zx.generator;

import java.io.*;

/**
 * @program: hadoop_demo1
 * @description: 操作文件的工具
 * @author: Ling
 * @create: 2018-12-19 17:48
 **/
public class FileUtil {

    static void showAllRowNum(String filePath) throws IOException {
        BufferedReader br = getBufferedReaderByPath(filePath);
        String tmp;
        int i = 0;
        while ((tmp = br.readLine()) != null) {
            i++;
        }
        System.out.println("文件一共有：" + i + " 行");
    }

    static void showSomeRow(String filePath, int startNum,int endNum) throws IOException {
        BufferedReader br = getBufferedReaderByPath(filePath);
        String tmp;
        int i = 0;
        while ((tmp = br.readLine()) != null) {
            if (i < endNum && i > startNum) {
                System.out.println(tmp);
            }
            i++;
        }
    }

    static BufferedReader getBufferedReaderByPath(String filePath) throws FileNotFoundException {
        BufferedReader br = new BufferedReader(new InputStreamReader(
                new FileInputStream(new File(filePath))));
        return br;
    }

    static BufferedOutputStream getBufferedOutputStream(String filePath,boolean append) throws FileNotFoundException {
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(new File(filePath),append));
        return bos;
    }

    /**
     * @param stringBuilder 需要输出的文件
     * @param outputPath 输出文件路径
     * @param append 是否追加
     * @throws IOException 文件io错误
     * @description: 数据追加形式进入文件
     */
    static void writeDataToFile(StringBuilder stringBuilder, String outputPath,boolean append) throws IOException {
        BufferedOutputStream bos = getBufferedOutputStream(outputPath,append);
        bos.write(stringBuilder.toString().getBytes());
    }

}
