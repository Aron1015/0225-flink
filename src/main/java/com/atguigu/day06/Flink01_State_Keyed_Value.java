package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink01_State_Keyed_Value {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] words = value.split(" ");
                return new WaterSensor(words[0], Long.parseLong(words[1]), Integer.parseInt(words[2]));
            }
        });

        //检测传感器的水位线值，如果连续的两个水位线差值超过10，就输出报警
        waterSensorDS.keyBy("id")
                .process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {
                    private ValueState<Integer> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //初始化
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("valueState", Integer.class));

                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        Integer lastVc = valueState.value() == null ? 0 : valueState.value();
                        if (Math.abs(value.getVc() - lastVc) >= 10) {
                            out.collect("报警");
                        }
                        valueState.update(value.getVc());
                    }
                }).print();

        env.execute();
    }
}
