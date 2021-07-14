package com.atguigu.day02.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class Flink02_Source_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> textFile = env.readTextFile("F:\\0225bigdata\\0225-flink\\input\\word.txt");

        textFile.print();
        env.execute();
    }
}
