package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink06_Timer_Exe_WithKeyedState {
    public static void main(String[] args) throws Exception {
        //1.流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3.对数据进行处理，封装成WaterSensor
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(" ");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorDStream.keyBy("id");

        //监控水位传感器的水位值，如果水位值在五秒钟之内(processing time)连续上升，则报警
        SingleOutputStreamOperator<WaterSensor> result = keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, WaterSensor>() {
            //用来记录上次水位高度
            private ValueState<Integer> valueState;
            //用来记录定时器时间
            private ValueState<Long> timerTs;


            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("valueState1", Integer.class,Integer.MIN_VALUE));
                timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("valueState", Long.class,Long.MIN_VALUE));
            }


            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {

                //判断当前水位是否高于上次
                if (value.getVc() > valueState.value()) {
                    //判断定时器是否重置
                    if (timerTs.value() == Long.MIN_VALUE) {
                        //注册定时器
                        System.out.println("注册定时器。。。");
                        timerTs.update(ctx.timerService().currentProcessingTime()+5000);
                        ctx.timerService().registerProcessingTimeTimer(timerTs.value());

                    }
                } else {
                    System.out.println("删除定时器。。。");
                    //删除定时器
                    ctx.timerService().deleteProcessingTimeTimer(timerTs.value());
                    //初始化定时器
                    timerTs.clear();;
                }
                //更新水位
                valueState.update(value.getVc());
                out.collect(value);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                ctx.output(new OutputTag<String>("output"){}, ctx.getCurrentKey() + "报警");
                //重置定时器
                timerTs.clear();
            }
        });

        result.print("主线");
        result.getSideOutput(new OutputTag<String>("output"){}).print("副线");

        env.execute();
    }
}
