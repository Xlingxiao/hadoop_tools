package com.zx.test;

import java.io.*;
import java.util.Scanner;

/**
 * @program: hadoop_demo1
 * @description: 阅读超大文件
 * @author: Ling
 * @create: 2018-12-09 14:00
 **/
public class ReadFile {
    public static void main(String[] args) throws IOException {
//        showHead("E:\\作业\\实训\\UserBehavior.csv", 100);
        showHead("E:\\作业\\实训\\数据\\dataset\\user_view.txt", 100);
//        showHead("E:\\作业\\实训\\数据\\dataset\\user_pay.txt", 100);
//        showRowNum("E:\\作业\\实训\\数据\\dataset\\user_pay.txt");
//        choiceData("E:\\作业\\实训\\数据\\dataset\\user_view.txt","201467");
//        choiceData("E:\\作业\\实训\\数据\\dataset\\user_pay.txt","2014673");
    }

    static void showHead(String filePath,int rowNum) throws IOException {
        BufferedReader br = getFileBuffer(filePath);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < rowNum; i++) {
            sb.append(br.readLine()).append("\n");
        }
        System.out.println(sb.toString());
    }

    static void choiceData(String filePath, String choice) throws IOException {
        BufferedReader br = getFileBuffer(filePath);
        StringBuilder sb = new StringBuilder();
        String line;
        int i = 0;
        while ((line =br.readLine()) != null) {
            if (!line.contains(choice))
                continue;
            sb.append(line).append("\n");
            if (++i>200)
                break;
        }
        System.out.println(sb.toString());
    }

    static BufferedReader getFileBuffer(String filePath) throws FileNotFoundException {
        File target = new File(filePath);
        FileInputStream tmpIS = new FileInputStream(target);
        InputStreamReader isr = new InputStreamReader(tmpIS);
        BufferedReader br = new BufferedReader(isr);
        return br;
    }

    static void showRowNum(String filePath) throws IOException {
        long num = 0;
        BufferedReader br = new BufferedReader(new InputStreamReader(
                new FileInputStream(new File(filePath))));
        while (br.readLine() != null) {
            num++;
        }
        System.out.println(filePath + "  文件有：" + num + "行");
    }
}
