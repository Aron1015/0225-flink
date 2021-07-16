package com.atguigu.day04.project;

import com.atguigu.bean.OrderEvent;
import com.atguigu.bean.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class Flink07_Project_Order {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> orderSource = env.readTextFile("F:\\0225bigdata\\0225-flink\\input\\OrderLog.csv");
        DataStreamSource<String> receiptSource = env.readTextFile("F:\\0225bigdata\\0225-flink\\input\\ReceiptLog.csv");

        SingleOutputStreamOperator<OrderEvent> orderDStream = orderSource.map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String value) throws Exception {
                String[] words = value.split(",");
                return new OrderEvent(Long.parseLong(words[0]), words[1], words[2], Long.parseLong(words[3]));
            }
        });

        SingleOutputStreamOperator<TxEvent> receiptDStream = receiptSource.map(new MapFunction<String, TxEvent>() {
            @Override
            public TxEvent map(String value) throws Exception {
                String[] words = value.split(",");
                return new TxEvent(words[0], words[1], Long.parseLong(words[2]));
            }
        });

        ConnectedStreams<OrderEvent, TxEvent> orderTxConnectedStreams = orderDStream.connect(receiptDStream);

        orderTxConnectedStreams.keyBy("txId","txId")
                .process(new CoProcessFunction<OrderEvent, TxEvent, String>() {
                    HashMap<String,OrderEvent> orderMap = new HashMap<>();
                    HashMap<String,TxEvent> txMap = new HashMap<>();

                    @Override
                    public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
                        if (txMap.containsKey(value.getTxId())) {
                            out.collect(value.getOrderId()+"已核对");
                            txMap.remove(value.getTxId());
                        }else {
                            orderMap.put(value.getTxId(), value);
                        }
                    }

                    @Override
                    public void processElement2(TxEvent value, Context ctx, Collector<String> out) throws Exception {
                        if (orderMap.containsKey(value.getTxId())) {
                            out.collect(orderMap.get(value.getTxId()).getOrderId()+"已核对");
                            orderMap.remove(value.getTxId());
                        }else {
                            txMap.put(value.getTxId(), value);
                        }
                    }
                }).print();

        env.execute();
    }
}
