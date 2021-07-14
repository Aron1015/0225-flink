package com.atguigu.day02.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink04_TransForm_KeyBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        /*env.socketTextStream("hadoop102", 9999)
                .keyBy(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String value) throws Exception {
                        String[] words = value.split(" ");
                        return Integer.parseInt(words[1]) % 2 == 0 ? "偶数" : "奇数";
                    }
                }).print();*/
        
        env.socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] words = value.split(" ");
                        return new WaterSensor(words[0],Long.parseLong(words[1]),Integer.parseInt(words[2]));
                    }
                })
                .keyBy("id").print();

        env.execute();
    }
}
