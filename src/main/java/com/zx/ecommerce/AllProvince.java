package com.zx.ecommerce;

import java.io.*;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * @program: hadoop_demo1
 * @description: 获得所有省份
 * @author: Ling
 * @create: 2018-12-12 15:01
 **/
public class AllProvince {
    public static void main(String[] args) throws IOException {
        String path = "E:\\作业\\实训\\数据\\eco2.csv";
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
        String tmp;
        Set<String> province = new LinkedHashSet<>();
        while ((tmp = br.readLine()) != null) {
            province.add(tmp.split(",", 6)[4].substring(0,2));
        }
        for (String s : province) {
            System.out.print(s + ",");
        }
    }
}
