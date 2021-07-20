package com.atguigu.day07;

import com.atguigu.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class Flink14_CEP_Project_OrderWatch {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KeyedStream<OrderEvent, Long> keyedStream = env.readTextFile("input/OrderLog.csv")
                .map(new MapFunction<String, OrderEvent>() {
                    @Override
                    public OrderEvent map(String value) throws Exception {
                        String[] words = value.split(",");
                        return new OrderEvent(Long.parseLong(words[0]), words[1], words[2], Long.parseLong(words[3]));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                            @Override
                            public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                                return element.getEventTime() * 1000;
                            }
                        }))
                .keyBy(OrderEvent::getOrderId);

        //统计创建订单到下单中间超过15分钟的超时数据以及正常的数据。

        //定义模式
        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("创建订单")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "create".equals(value.getEventType());
                    }
                })
                .followedBy("下单")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "pay".equals(value.getEventType());
                    }
                })
                .within(Time.minutes(15));

        PatternStream<OrderEvent> stream = CEP.pattern(keyedStream, pattern);

        SingleOutputStreamOperator<String> result = stream.select(new OutputTag<String>("超时") {
        }, new PatternTimeoutFunction<OrderEvent, String>() {
            @Override
            public String timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
                return pattern.toString();
            }
        }, new PatternSelectFunction<OrderEvent, String>() {
            @Override
            public String select(Map<String, List<OrderEvent>> pattern) throws Exception {
                return pattern.toString();
            }
        });

        result.print("主线");
        result.getSideOutput(new OutputTag<String>("超时"){}).print("副线");

        env.execute();
    }
}
