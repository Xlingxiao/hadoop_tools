package com.zx.test;

/**
 * @program: hadoop_demo1
 * @description:
 * @author: Ling
 * @create: 2018-11-30 11:13
 **/
public class Main {
    public static void main(String[] args) {
        int flag = 126;
        for (int i = 0; i < 7; i++) {
            int tmp = 1 << i;
//            System.out.println(tmp & flag);
            if ((tmp & flag) != 0) {
                System.out.println(i + 1);
            }
        }
    }
}
