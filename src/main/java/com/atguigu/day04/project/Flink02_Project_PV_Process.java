package com.atguigu.day04.project;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Flink02_Project_PV_Process {
    private static Long count =0L;
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.readTextFile("F:\\0225bigdata\\0225-flink\\input\\UserBehavior.csv");
        
        dataStreamSource.process(new ProcessFunction<String, Tuple2<String,Long>>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] words = value.split(",");
                if ("pv".equals(words[3])) {
                    count++;
                    out.collect(Tuple2.of(words[3],count));
                }
            }
        }).print();

        env.execute();
    }
}
