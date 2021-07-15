package com.atguigu.day02.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink07_TransForm_Union {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取数据源
        DataStreamSource<String> source1 = env.fromElements("1", "2", "3", "4", "5", "6");

        DataStreamSource<String> source2 = env.fromElements("a", "b", "c", "d", "e", "f");

        DataStreamSource<String> source3 = env.fromElements("22", "33", "44", "55", "66", "77");
        //TODO 3.使用Union连接两条流
        DataStream<String> union = source1.union(source2,source3);

        union.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).print();


        env.execute();
    }
}
