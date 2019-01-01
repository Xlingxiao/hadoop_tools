package com.zx.generator;

import java.io.*;
import java.util.Calendar;

/**
 * @program: hadoop_demo1
 * @description: 将两个文件合成一个
 * @author: Ling
 * @create: 2018-12-09 21:58
 **/
public class Compound {
    public static void main(String[] args) throws IOException {
//        待合成文件行数
        int rowNum = 1334282;
        long startTime = Calendar.getInstance().getTimeInMillis();
        Main main = new Main();
        main.start(rowNum);
        String filePath1 = "E:\\作业\\实训\\数据\\addData.csv";
        String filePath2 = "E:\\作业\\实训\\数据\\output\\电商订单分析.txt";
        File file1 = new File(filePath1);
        File file2 = new File(filePath2);
        BufferedReader br1 = new BufferedReader(new InputStreamReader(new FileInputStream(file1)));
        BufferedReader br2 = new BufferedReader(new InputStreamReader(new FileInputStream(file2)));
        String tmp1;
        String tmp2;
        StringBuilder stringBuilder = new StringBuilder();
        while ((tmp1 = br1.readLine()) != null) {
            tmp2 = br2.readLine();
            stringBuilder.append(tmp2).append(",").append(tmp1).append("\n");
        }
        File outputFile = new File("E:\\作业\\实训\\数据\\output\\ecommerceE.csv");
        BufferedOutputStream bo = new BufferedOutputStream(new FileOutputStream(outputFile));
        bo.write(stringBuilder.toString().getBytes());
        long endTime = Calendar.getInstance().getTimeInMillis();
        System.out.println("总共用时：" + (endTime - startTime));
    }
}
