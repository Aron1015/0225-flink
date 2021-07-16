package com.atguigu.day04.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Flink08_Window_Time_Tumbling_Fun_Process {
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
                .process(new ProcessWindowFunction<Tuple2<String, Long>, Long, Tuple, TimeWindow>() {
                    //自定义的累加器
                    Long count =0l;
                    //TODO 使用全窗口函数，显示单词累加的功能
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Long> out) throws Exception {
                        System.out.println("process...");
                        for (Tuple2<String, Long> element : elements) {
                            count++;
                        }
                        out.collect(count);
                    }
                }).print();

        env.execute();
    }
}
