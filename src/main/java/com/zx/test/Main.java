package com.zx.test;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @program: hadoop_demo1
 * @description:
 * @author: Ling
 * @create: 2018-11-30 11:13
 **/
public class Main {
    public static void main(String[] args) {
        String msg = "房屋, 卫士, 自流平, 美, 缝, 剂, 瓷砖, 地砖, 专用, 双, 组份, 真, " +
                "瓷, 胶, 防水, 填缝剂, 镏, 金色" +
                ",房屋, 卫士, 自流平, 美, 缝, 剂,瓷砖,地砖,专用";
        String[] data = msg.split(",");
        Map<String, Integer> map = new HashMap<>();
        for (String datum : data) {
            if (map.containsKey(datum)) {
                map.put(datum, map.get(datum) + 1);
            }else
                map.put(datum, 1);
        }
        System.out.println(map.toString());

        Map<String, Integer> result = map.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (oldValue, newValue) -> oldValue, LinkedHashMap::new));
        System.out.println(result);
    }
}
