package com.atguigu.day04.project;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Flink06_Project_Ads_Click {
    private static Long count =0L;
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.readTextFile("F:\\0225bigdata\\0225-flink\\input\\AdClickLog.csv");

        dataStreamSource.process(new ProcessFunction<String, Tuple2<Tuple2<String,Long>,Long>>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<Tuple2<String, Long>, Long>> out) throws Exception {
                String[] words = value.split(",");
                out.collect(Tuple2.of(Tuple2.of(words[2],Long.parseLong(words[1])),1L));
            }
        })
                .keyBy(0)
                .sum(1)
                .print("省份_广告");


        env.execute();
    }
}
