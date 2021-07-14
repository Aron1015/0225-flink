package com.atguigu.day02.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink03_Source_Socket {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("hadoop102", 9999).print();
        env.execute();
    }
}
