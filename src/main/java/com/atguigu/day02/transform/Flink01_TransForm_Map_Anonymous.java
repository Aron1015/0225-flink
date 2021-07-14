package com.atguigu.day02.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink01_TransForm_Map_Anonymous {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("hadoop102", 9999)
        .map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] words = value.split(",");
                return new WaterSensor(words[0], Long.parseLong(words[1]), Integer.parseInt(words[2]));
            }
        }).print();

        env.execute();
    }
}
