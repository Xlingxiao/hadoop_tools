package com.zx.generator;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @program: hadoop_demo1
 * @description: pc或者移动端
 * @author: Ling
 * @create: 2018-12-10 09:52
 **/
public class POM {
    static List<PAV> PAVs = new ArrayList<>();

    static {
        initProbability();
    }

    private static class PAV{
        int min;
        int max;
        String way;

        PAV(String way, int min, int max) {
            this.way = way;
            this.min = min;
            this.max = max;
        }
    }

    private static void initProbability(){
        PAVs.add(new PAV("移动端", 0, 27));
        PAVs.add(new PAV("PC端", 27, 100));
    }

    public String getWay(){
        int i = (int) (Math.random() * 99 + 1);
        return getPAV(i).way;
    }

    private PAV getPAV(int i) {
        for (PAV pav : PAVs) {
            if (pav.min <= i && pav.max > i)
                return pav;
        }
        return PAVs.get(1);
    }

    @Test
    public void test(){
        for (int i = 0; i < 200; i++) {
            System.out.println(getWay());
        }
    }
}
