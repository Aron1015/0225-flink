package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink02_WordCount_Bounded {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);

        DataStreamSource<String> lineDS = senv.readTextFile("F:\\0225bigdata\\0225-flink\\input\\word.txt");

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });
        KeyedStream<Tuple2<String, Integer>, Tuple> keyBy = wordToOneDS.keyBy(0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyBy.sum(1);

        sum.print();
        senv.execute();
    }
}
