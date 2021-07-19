package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Flink04_Window_EventTime_Tumbling_Custom_WaterMark_onPeriodic {
    public static void main(String[] args) throws Exception {
        //1.流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 手动设置生成WaterMark间隔时间 默认是200毫秒
        env.getConfig().setAutoWatermarkInterval(1000);
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

        //TODO 自定义WaterMark
        SingleOutputStreamOperator<WaterSensor> timestampsAndWatermarks = waterSensorDStream.assignTimestampsAndWatermarks(new WatermarkStrategy<WaterSensor>() {
            @Override
            public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new MyWaterMarkOnPeriodic(Duration.ofSeconds(2));
            }
        }
        //分配时间戳
        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>(){

            @Override
            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                return element.getTs()*1000;
            }
        }));

        KeyedStream<WaterSensor, String> keyedStream = timestampsAndWatermarks.keyBy(WaterSensor::getId);

        //5.开启基于事件时间的滚动窗口
        WindowedStream<WaterSensor, String, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));

        //6.处理
        window.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
            @Override
            public void process(String key, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                String msg = "当前key: " + key
                        + "窗口: [" + context.window().getStart() / 1000 + "," + context.window().getEnd()/1000 + ") 一共有 "
                        + elements.spliterator().estimateSize() + "条数据 ";
                out.collect(msg);
            }
        }).print();

        env.execute();
    }

    public static class MyWaterMarkOnPeriodic implements WatermarkGenerator<WaterSensor>{
        /** The maximum timestamp encountered so far. */
        private long maxTimestamp;

        /** The maximum out-of-orderness that this watermark generator assumes. */
        private long outOfOrdernessMillis;

        public MyWaterMarkOnPeriodic(Duration maxOutOfOrderness){
            this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();

            // start so that our lowest watermark would be Long.MIN_VALUE.
            this.maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;

        }

        //间歇性生成WaterMark
        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
        }

        //周期性生成WaterMark
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            System.out.println("生成WaterMark。。。");
            output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1));
        }
    }
}