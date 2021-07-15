package com.atguigu.day02.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink08_TransForm_Agg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] words = value.split(" ");
                        return new WaterSensor(words[0],Long.parseLong(words[1]),Integer.parseInt(words[2]));
                    }
                })
                .keyBy(WaterSensor::getId)
//                .sum("vc")
                .max("vc")
//                .min("vc")
//                .maxBy("vc",false)
                .print();


        env.execute();
    }
}
