package com.ggc.javaapitest;

import scala.Tuple2;

import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiConsumer;

public class TreeMapDemo {
    @SuppressWarnings("ComparatorMethodParameterNotUsed")
    public static void main(String[] args) {


        TreeMap<Integer, Tuple2<String, Integer>> treeMap =
                new TreeMap<>((o1, o2) -> o1 > o2 ? 1 : -1);
        treeMap.put(8, new Tuple2<>("hh", 1));
        treeMap.put(6, new Tuple2<>("ff", 6));
        treeMap.put(5, new Tuple2<>("ee", 5));
        treeMap.put(9, new Tuple2<>("i", 9));

        treeMap.forEach((integer, stringIntegerTuple2) -> System.out.println("key = " + integer + " , value = " + stringIntegerTuple2.toString()));

        Map.Entry<Integer, Tuple2<String, Integer>> pollLastEntry = treeMap.pollLastEntry();
        System.out.println("pollLastEntry = " + pollLastEntry);

        treeMap.forEach((integer, stringIntegerTuple2) -> System.out.println("key = " + integer + " , value = " + stringIntegerTuple2.toString()));


    }
}
