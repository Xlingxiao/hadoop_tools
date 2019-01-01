package com.zx.generator;

import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @program: hadoop_demo1
 * @description: 生成时间段
 * @author: Ling
 * @create: 2018-12-09 19:29
 **/
public class PayTimeUtil {

    private static final long DAYTIME = 86400000;
    private static final long TIME20180801 = 1533052800000L;
    static List<ProbabilityAndValue> probability = new ArrayList<>();

    static {
        initProbability();
    }

    /**
     * 0-8 6%   8-12  18%    12-18 28%    18-21 29%    21-24 19%
     * 随机生成100以内的数，0-6
     * @return 一天内的小时分钟的毫秒数
     */
    private static class ProbabilityAndValue{
        int pmin;
        int pmax;
        int min;
        int max;

        ProbabilityAndValue(int pmin, int pmax, int min, int max) {
            this.pmin = pmin;
            this.pmax = pmax;
            this.min = min;
            this.max = max;
        }
    }

    private static void initProbability(){
        probability.add(new ProbabilityAndValue(0,5, 0, 1));
        probability.add(new ProbabilityAndValue(5,13, 1, 8));
        probability.add(new ProbabilityAndValue(13,27, 8, 12));
//        小提升
        probability.add(new ProbabilityAndValue(27,39, 12, 14));
        probability.add(new ProbabilityAndValue(39,53, 14, 18));
//        提升前缓冲
        probability.add(new ProbabilityAndValue(53,60, 18, 19));
        probability.add(new ProbabilityAndValue(60,90, 19, 22));
        probability.add(new ProbabilityAndValue(90,100, 22, 24));
    }

    public String getPayTime(){
        long tmp = getMonthNam() * DAYTIME + getHourNum();
        return long_Time(tmp);    }

    @Test
    public void test(){
        for (int i = 0; i < 147939; i++) {
            long tmp = getHourNum();
            SimpleDateFormat format = new SimpleDateFormat("HH");

        }
    }


    private long getHourNum(){
        int i = (int) (1 + Math.random() * 99);
        ProbabilityAndValue p = getProbabilityAndValue(i);
        return dayTime(p.min, p.max);
    }

//    根据数字返回probability
    private ProbabilityAndValue getProbabilityAndValue(int i) {
        for (ProbabilityAndValue probabilityAndValue : probability) {
            if (i>=probabilityAndValue.pmin && i < probabilityAndValue.pmax)
                return probabilityAndValue;
        }
        return probability.get(3);
    }

//    根据数字生成随机数 6-8点之间的时间应该是 6点的毫秒数+（8-6）*每小时的毫秒数
    private long dayTime(int min,int max) {
        return (long) ((Math.random() * DAYTIME / 24) * (max - min)) + DAYTIME * min / 24;
    }

//    纯随机一个月中的第几天
    private long getMonthNam(){
        return (long) (1 + Math.random() * 30);
    }

//    根据long值生成字符串类型的时间
    private String long_Time(long time) {
        Date test = new Date(TIME20180801 + time);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
//        System.out.println(sdf.format(test));
        return sdf.format(test);
    }
}
