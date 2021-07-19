package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Flink02_State_Keyed_ListState {
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

        //针对每个传感器输出最高的3个水位值
        waterSensorDS.keyBy("id")
                .process(new KeyedProcessFunction<Tuple, WaterSensor, List<Integer>>() {
                    private ListState<Integer> listState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        listState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("listState", Integer.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<List<Integer>> out) throws Exception {
                        ArrayList<Integer> list = new ArrayList<>();

                        //将当前数据保存到状态中
                        listState.add(value.getVc());

                        //取出状态中的数据
                        Iterable<Integer> integers = listState.get();

                        //将状态中的数据保存到list集合中
                        for (Integer integer : integers) {
                            list.add(integer);
                        }
                        list.sort(new Comparator<Integer>() {
                            @Override
                            public int compare(Integer o1, Integer o2) {
                                return o2-o1;
                            }
                        });
                        if (list.size() > 3) {
                            list.remove(3);
                        }
                        listState.update(list);

                        out.collect(list);
                    }
                }).print();

        env.execute();
    }
}
