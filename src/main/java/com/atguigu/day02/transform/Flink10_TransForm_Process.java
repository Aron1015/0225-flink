package com.atguigu.day02.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Flink10_TransForm_Process {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("hadoop102", 9999)
                //实现map操作
                .process(new ProcessFunction<String, WaterSensor>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<WaterSensor> out) throws Exception {
                        String[] words = value.split(" ");
                        out.collect(new WaterSensor(words[0],Long.parseLong(words[1]),Integer.parseInt(words[2])));
                    }
                })
                /*.process(new ProcessFunction<WaterSensor, Tuple2<String,Integer>>() {
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        out.collect(new Tuple2<>(value.getId(),value.getVc()));
                    }
                })
                .print();*/
                .keyBy(WaterSensor::getId)
                //在keyBay之后使用process处理
                .process(new KeyedProcessFunction<String, WaterSensor, Tuple2<String,Integer>>() {
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        out.collect(new Tuple2<>("key是："+ctx.getCurrentKey(),value.getVc()));
                    }
                })
                .print();


        env.execute();
    }
}
