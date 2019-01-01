package com.zx.test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @program: hadoop_demo1
 * @description:
 * @author: Ling
 * @create: 2018-12-26 17:09
 **/
@SuppressWarnings("Duplicates")
public class Test {
    String[] KINDS = new String[]{"休闲家具", "内衣会场", "医药健康", "品质建材", "图书乐器会场",
            "大家电会场", "女装会场", "女装商场同款", "女装风格好店", "女鞋会场", "家纺家饰会场",
            "小家电会场", "手机会场", "手表眼镜", "数码家电会场", "母婴主会场", "洗护清洁会场",
            "潮酷数码会场", "灯具灯饰会场", "珠宝饰品", "生鲜会场", "电脑办公会场", "男装会场",
            "男装风格好店", "百货会场", "童装会场", "箱包配饰", "精品家具", "美妆主会场",
            "车品配件", "运动户外", "进口尖货", "零食会场", "食品主会场", "高端洗护会场"};

    private class Kind {
        String name;
        long countUser;
        long more;

        public Kind(String name) {
            this.name = name;
            this.countUser = 0;
            this.more = 0;
        }

        @Override
        public String toString() {
            return "Kind{" +
                    "name='" + name + '\'' +
                    ", countUser=" + countUser +
                    ", more=" + more +
                    '}';
        }
    }
    /*判断列表中是否有超过两个相同标签的*/
    private boolean judge(String[] data, String target) {
        int i = 0;
        for (String datum : data)
            if (datum.equals(target)) {
                i++;
            }
        if (i > 1) {
            return true;
        } else return false;
    }
    //        根据名字返回种类
    private Kind getKind(String name) {
        for (Kind kind : list) {
            if (kind.name.equals(name)) {
                return kind;
            }
        }
        return null;
    }

    List<Kind> list = new ArrayList<>();
    @org.junit.Test
    public void test(){
        String target = "手表眼镜\t母婴主会场\t手表眼镜\t珠宝饰品\t手表眼镜\t内衣会场\t男装会场\t" +
                "母婴主会场\t珠宝饰品\t男装会场\t男装会场\t手表眼镜\t女装会场\t女装风格好店\t女装商场同款" +
                "\t男装会场\t童装会场\t女装会场\t女装会场\t家纺家饰会场\t内衣会场\t女装会场\t童装会场\t" +
                "母婴主会场\t男装会场\t女装会场\t品质建材\t女装会场\t运动户外\t童装会场\t内衣会场\t箱包配饰\t" +
                "珠宝饰品\t女装风格好店\t女装商场同款\t电脑办公会场\t运动户外\t图书乐器会场\t";
        for (String kind : KINDS) {
            list.add(new Kind(kind));
        }
        String[] data = target.split("\t");
        List<String> tmpList = new LinkedList<>();
        Kind tmp ;
        for (String datum : data) {
            tmp = getKind(datum);
            if (tmp == null) continue;
            tmp.countUser++;
            if (tmpList.contains(datum)) continue;
            if (judge(data, datum)) {
                tmp.more++;
                tmpList.add(datum);
            }
        }

        for (Kind kind : list) {
            System.out.println(kind);
        }
    }
}
