package com.atguigu.day02.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink01_TransForm_Map_StaticClass {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env.socketTextStream("hadoop102", 9999)
                .map(new MyRichMapFunction())
                .print();

        env.execute();
    }

    private static class MyMapFunction implements MapFunction<Integer,Integer> {

        @Override
        public Integer map(Integer value) throws Exception {
            return value*value;
        }
    }

    private static class MyRichMapFunction extends RichMapFunction<String, String> {
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("打开");
        }

        @Override
        public void close() throws Exception {
            System.out.println("结束连接");
        }

        @Override
        public String map(String value) throws Exception {
            System.out.println("加载中。。");
            return value;
        }
    }
}
