package com.atguigu.exercise;

import com.atguigu.bean.HotItem;
import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;

public class Flink03_Project_Product_TopN {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.readTextFile("input/UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] words = value.split(",");
                        return new UserBehavior(Long.parseLong(words[0]), Long.parseLong(words[1]), Integer.parseInt(words[2]), words[3], Long.parseLong(words[4]));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                return element.getTimestamp() * 1000;
                            }
                        }))
                .filter(new FilterFunction<UserBehavior>() {
                    @Override
                    public boolean filter(UserBehavior value) throws Exception {
                        return "pv".equals(value.getBehavior());
                    }
                })
                .keyBy(UserBehavior::getItemId)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new AggregateFunction<UserBehavior, Long, Long>() {
                               @Override
                               public Long createAccumulator() {
                                   return 0L;
                               }

                               @Override
                               public Long add(UserBehavior value, Long accumulator) {
                                   return accumulator + 1L;
                               }

                               @Override
                               public Long getResult(Long accumulator) {
                                   return accumulator;
                               }

                               @Override
                               public Long merge(Long a, Long b) {
                                   return a + b;
                               }
                           },
                        new ProcessWindowFunction<Long, HotItem, Long, TimeWindow>() {
                            @Override
                            public void process(Long aLong, Context context, Iterable<Long> elements, Collector<HotItem> out) throws Exception {
                                out.collect(new HotItem(aLong,elements.iterator().next(),context.window().getEnd()));
                            }
                        })
                .keyBy(HotItem::getWindowEndTime)
                .process(new KeyedProcessFunction<Long, HotItem, String>() {
                    private ListState<HotItem> hotItems;
                    private ValueState<Long> triggerTS;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hotItems = getRuntimeContext().getListState(new ListStateDescriptor<HotItem>("listState", HotItem.class));
                        triggerTS = getRuntimeContext().getState(new ValueStateDescriptor<Long>("valueState", Long.class));
                    }

                    @Override
                    public void processElement(HotItem value, Context ctx, Collector<String> out) throws Exception {
                        hotItems.add(value);
                        if (triggerTS.value() == null) {
                            ctx.timerService().registerEventTimeTimer(value.getWindowEndTime()+1L);
                            triggerTS.update(value.getWindowEndTime());
                        }

                    }

                    //等属于某个窗口所有的数据来了之后再计算TopN
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        Iterable<HotItem> hotItems = this.hotItems.get();

                        // 存储最终的结果
                        ArrayList<HotItem> result = new ArrayList<>();
                        for (HotItem hotItem : hotItems) {
                            result.add(hotItem);
                        }
                        this.hotItems.clear();
                        triggerTS.clear();

                        // 对result 排序取前3
                        result.sort((o1, o2) -> o2.getCount().intValue() - o1.getCount().intValue());

                        StringBuilder sb = new StringBuilder();
                        sb.append("窗口结束时间: " + (timestamp - 1) + "\n");
                        sb.append("---------------------------------\n");
                        for (int i = 0; i < 3; i++) {
                            sb.append(result.get(i) + "\n");
                        }
                        sb.append("---------------------------------\n\n");
                        out.collect(sb.toString());
                    }
                }).print();

        env.execute();
    }
}
