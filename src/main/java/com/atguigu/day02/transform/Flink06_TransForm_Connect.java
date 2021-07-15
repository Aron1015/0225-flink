package com.atguigu.day02.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class Flink06_TransForm_Connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> streamSource1 = env.fromElements(1, 2, 3, 4, 5, 6,7,8,9);
        DataStreamSource<String> streamSource2 = env.fromElements("a","b","c","d");

        ConnectedStreams<Integer, String> connect = streamSource1.connect(streamSource2);

        connect.getFirstInput().print();
        connect.getSecondInput().print();

        System.out.println("=====================");
        
        connect.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return value+"map1";
            }

            @Override
            public String map2(String value) throws Exception {
                return value+"map2";
            }
        }).print();

        
        env.execute();
    }
}
