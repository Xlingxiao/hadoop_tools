package com.zx.test;

import java.io.*;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * @program: hadoop_demo1
 * @description:
 * @author: Ling
 * @create: 2018-12-12 17:05
 **/
@SuppressWarnings("ALL")
public class changeFile {

    static String[] Province = new String[]{"北京", "天津", "上海", "重庆", "河北",
            "山西", "辽宁", "吉林", "黑龙江", "江苏", "浙江", "安徽", "福建", "江西", "山东",
            "河南", "湖北", "湖南", "广东", "海南", "四川", "贵州", "云南", "陕西", "甘肃",
            "青海", "西藏", "广西", "内蒙古", "宁夏", "新疆", "香港", "澳门"};

    private static class Node{
        String name;
        int id;
        int category;

        public Node(String name, int id, int category) {
            this.name = name;
            this.id = id;
            this.category = category;
        }

        @Override
        public String toString() {
            return "{" +
                    "\"name\":\"" + name + '\"' +
                    ", \"id\":" + id +
                    ", \"category\":" + category +
                    "},";
        }
    }
    private static class Link{
        String source;
        String target;
        String value;

        @Override
        public String toString() {
            return "{" +
                    "\"source\":\"" + source + '\"' +
                    ", \"target\":\"" + target + '\"' +
                    ", \"value\":\"" + value + '\"' +
                    "},";
        }

        public Link(String source, String target, String value) {
            this.source = source;
            this.target = target;
            this.value = value;
        }
    }

//    所有节点
    static Set<Node> set = new HashSet<>();
//    所有关系
    static List<String[]> list = new LinkedList<>();
    //    所有边
    static List<Link> links = new LinkedList<>();
    public static void main(String[] args) throws IOException {
        File file = new File("E:\\作业\\实训\\数据\\output\\fake.csv");
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
        String tmp;
//        每行数据
        StringBuilder sb = new StringBuilder();
        Set<String> one = new HashSet<>();
        int id = 1;
        while ((tmp = br.readLine()) != null) {
//            String[] data = tmp.split("\t");
//            sb.append(data[0]).append(",").append(data[1]).append("\n");
//            tmp = tmp.replace("\t", "");
//            tmp = tmp.substring(2);
//            String[] adata = tmp.split(",");
//            for (String datum : adata) {
//                one.add(datum);
//            }
            sb.append(tmp).append("\n");
            tmp = tmp.substring(0, 2);
            one.add(tmp);
        }
        for (String s : one) {
            set.add(new Node(s, id++, 0));
        }
//        for (String s : Province) {
//            set.add(new Node(s, id++, 1));
//        }
//        遍历所有节点
        for (Node node : set) {
            System.out.println(node.toString());
        }
//        将关系放在这里面
        String[] data = sb.toString().split("\n");
        for (String datum : data) {
            list.add(datum.split(","));
        }

        /**
         * 原始
        for (String[] strings : list) {
//            广西
            String name = strings[0];
            int sourceid = 0;
            for (Node node : set) {
                if (name.equals(node.name)) {
                    sourceid = node.id;
                }
            }
            for (int i = 1; i < strings.length; i++) {
//                珠宝,珍珠,产后
                int targetid = 0;
                for (Node node : set) {
                    if (strings[i].equals(node.name)) {
                        targetid = node.id;
                        links.add(new Link(String.valueOf(sourceid),String.valueOf(targetid),"特产"));
                    }
                }
            }
        }
        */

        for (String[] strings : list) {
            if (strings.length<3)
                continue;
            int sourceid = 0;
//            获得原始id
            for (Node node : set) {
                if (node.name.equals(strings[0])) {
                    sourceid = node.id;
                }
            }
            int targetid = 0;
//            仿冒的id
            for (Node node : set) {
                if (node.name.equals(strings[2])) {
                    targetid = node.id;
                    break;
                }
            }
            links.add(new Link(String.valueOf(sourceid), String.valueOf(targetid), "冒充数量："+strings[1]));
        }

        for (Link link : links) {
            System.out.println(link.toString());
        }
    }


}
