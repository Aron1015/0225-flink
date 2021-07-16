package com.atguigu.day04.project;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class Flink03_Project_UV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.readTextFile("F:\\0225bigdata\\0225-flink\\input\\UserBehavior.csv");

        dataStreamSource.process(new ProcessFunction<String, Tuple2<String,Long>>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] words = value.split(",");
                if ("pv".equals(words[3])) {
                    out.collect(Tuple2.of("pv", Long.parseLong(words[0])));
                }
            }
        })
                .keyBy(0)
                .process(new KeyedProcessFunction<Tuple, Tuple2<String, Long>, Tuple2<String,Long>>() {
                    HashSet<Long> hashSet = new HashSet<>();
                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                        hashSet.add(value.f1);
                        out.collect(Tuple2.of("uv",(long)hashSet.size()));
                    }
                }).print();

        env.execute();
    }
}
