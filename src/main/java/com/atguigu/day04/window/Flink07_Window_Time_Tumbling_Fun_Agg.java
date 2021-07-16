package com.atguigu.day04.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Flink07_Window_Time_Tumbling_Fun_Agg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<Tuple2<String, Long>> map = streamSource.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                return Tuple2.of(value, 1L);
            }
        });

        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = map.keyBy(0);

        //TODO 创建基于时间的滚动窗口
        keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                //TODO 使用窗口增量聚和函数，显示单词累加的功能
                .aggregate(new AggregateFunction<Tuple2<String, Long>, Long, Long>() {
                    /**
                     * 初始化累加器
                     * @return
                     */
                    @Override
                    public Long createAccumulator() {
                        System.out.println("初始化");
                        return 0L;
                    }

                    /**
                     * 累加器操作
                     * @param value
                     * @param accumulator
                     * @return
                     */
                    @Override
                    public Long add(Tuple2<String, Long> value, Long accumulator) {
                        System.out.println("累加器操作");
                        return value.f1+accumulator;
                    }

                    /**
                     * 获取结果
                     * @param accumulator
                     * @return
                     */
                    @Override
                    public Long getResult(Long accumulator) {
                        System.out.println("获取结果");
                        return accumulator;
                    }

                    /**
                     * 累加器的合并，只有会话窗口才会调用
                     * @param a
                     * @param b
                     * @return
                     */
                    @Override
                    public Long merge(Long a, Long b) {
                        System.out.println("合并累加器");
                        return a+b;
                    }
                }).print();

        env.execute();
    }
}
