package com.zx.generator;

import java.io.BufferedReader;
import java.io.IOException;

import static com.zx.generator.FileUtil.*;

/**
 * @program: hadoop_demo1
 * @description: 将原数据扩展到十倍大小
 *  生成随机数 6-13 每条信息重复这个次数
 * @author: Ling
 * @create: 2018-12-19 17:46
 **/
public class ExtendData {

//    StringBuilder rowData = new StringBuilder();
    StringBuilder additionData = new StringBuilder();

    public static void main(String[] args) throws IOException {
        String filePath = "E:\\作业\\实训\\数据\\output\\电商订单分析.txt";
        ExtendData extendData = new ExtendData();
        extendData.start(filePath);
//        showSomeRow(filePath, 1258000, 1551828);
        showAllRowNum(filePath);
        System.out.println("处理完成！");
    }

    void start(String filePath) throws IOException {
        BufferedReader br = getBufferedReaderByPath(filePath);
        getData(br);
        writeDataToFile(additionData, filePath, false);
    }

    void getData(BufferedReader br) throws IOException {
//        订单id起始值
        int initValue = 10001;
//        商品id起始值
        int commodityId = 1000001;
        String tmpMsg ;
        while ((tmpMsg = br.readLine()) != null) {
//            rowData.append(tmpMsg).append("\n");
            tmpMsg = tmpMsg.split(",", 2)[1];
            int m = (int) (Math.random() * 11 + 4);
            for (int i = 0; i < m; i++) {
                additionData.append(initValue++).append(",");
                additionData.append(tmpMsg).append(",");
                additionData.append(commodityId);
                additionData.append("\n");
            }
            commodityId++;
        }
        br.close();
    }

}
