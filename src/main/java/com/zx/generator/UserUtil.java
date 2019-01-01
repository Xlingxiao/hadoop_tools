package com.zx.generator;

import org.junit.Test;

/**
 * @program: hadoop_demo1
 * @description: 创建用户用的
 * @author: Ling
 * @create: 2018-12-09 19:28
 **/
public class UserUtil {
    @Test
    public void test(){
        for (int i = 0; i < 10; i++) {
            System.out.println(getId());
        }
    }

    public String getId(){
        int id = (int) (10001 + Math.random() * 30000);
        return String.valueOf(id);
    }
}
