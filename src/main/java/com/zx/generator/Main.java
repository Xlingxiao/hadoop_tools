package com.zx.generator;

import java.io.*;

/**
 * @program: hadoop_demo1
 * @description: 将数据生成为单独的文件
 * @author: Ling
 * @create: 2018-12-09 19:27
 **/
public class Main {

    static BufferedOutputStream bos;
    static File outputFile;

    static {
        outputFile = new File("E:\\作业\\实训\\数据\\addData.csv");
        try {
            bos = new BufferedOutputStream(new FileOutputStream(outputFile));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
//        StringBuilder sb = new StringBuilder();
//        UserUtil userUtil = new UserUtil();
//        PayTimeUtil payTimeUtil = new PayTimeUtil();
//        PayViewsUtil payViewsUtil = new PayViewsUtil();
//        long startTime = Calendar.getInstance().getTimeInMillis();
//        for (int i = 0; i < 147939; i++) {
//            sb.append(userUtil.getId()).append(",");
//            sb.append(payTimeUtil.getPayTime()).append(",");
//            sb.append(payViewsUtil.getViewTimes()).append("\n");
////            sb.delete(0, sb.length());
//        }
//        outputFile(sb);
//        long endTime = Calendar.getInstance().getTimeInMillis();
//        System.out.println("总共用时：" + (endTime - startTime));
    }

    void start(int rowNum) throws IOException {
        StringBuilder sb = new StringBuilder();
        UserUtil userUtil = new UserUtil();
        PayTimeUtil payTimeUtil = new PayTimeUtil();
        PayViewsUtil payViewsUtil = new PayViewsUtil();
        POM pom = new POM();
        for (int i = 0; i < rowNum; i++) {
            sb.append(userUtil.getId()).append(",");
            sb.append(payTimeUtil.getPayTime()).append(",");
            sb.append(payViewsUtil.getViewTimes()).append(",");
            sb.append(pom.getWay()).append("\n");
//            sb.delete(0, sb.length());
        }
        outputFile(sb);

    }

    private static void outputFile(StringBuilder stringBuilder) throws IOException {
        bos.write(stringBuilder.toString().getBytes());
    }

}
