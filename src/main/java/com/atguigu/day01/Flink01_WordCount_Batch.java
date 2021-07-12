package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Flink01_WordCount_Batch {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> lineDS = env.readTextFile("F:\\0225bigdata\\0225-flink\\input\\word.txt");

        FlatMapOperator<String, String> wordToWordDS = lineDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });
        MapOperator<String, Tuple2<String, Integer>> wordToOneDS = wordToWordDS.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });

        UnsortedGrouping<Tuple2<String, Integer>> groupByWord = wordToOneDS.groupBy(0);
        AggregateOperator<Tuple2<String, Integer>> sumWord = groupByWord.sum(1);

        sumWord.print();
    }
}
