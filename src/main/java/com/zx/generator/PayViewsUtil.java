package com.zx.generator;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @program: hadoop_demo1
 * @description: 生成支付前查看次数
 * @author: Ling
 * @create: 2018-12-09 19:31
 **/
public class PayViewsUtil {

    static List<PAV> PAVs = new ArrayList<>();

    static {
        initProbability();
    }

    private static class PAV{
        int pmin;
        int pmax;
        int min;
        int max;

        PAV(int pmin, int pmax, int min, int max) {
            this.pmin = pmin;
            this.pmax = pmax;
            this.min = min;
            this.max = max;
        }
    }

    private static void initProbability(){
        PAVs.add(new PAV(1,4, 0, 17));
        PAVs.add(new PAV(4,10, 17, 64));
        PAVs.add(new PAV(10,15, 64, 97));
        PAVs.add(new PAV(21,30, 97, 100));
    }

    public int getViewTimes(){
        int i = (int) (Math.random() * 99 + 1);
        return getTimes(getPAV(i));
    }

    private PAV getPAV(int i) {
        for (PAV pav : PAVs) {
            if (pav.min <= i && pav.max > i)
                return pav;
        }
        return PAVs.get(2);
    }

    private int getTimes(PAV p) {
        return (int) ((Math.random() * (p.pmax - p.pmin)) + p.pmin);
    }

    @Test
    public void test(){
        for (int i = 0; i < 200; i++) {
            System.out.println(getViewTimes());
        }
    }
}
