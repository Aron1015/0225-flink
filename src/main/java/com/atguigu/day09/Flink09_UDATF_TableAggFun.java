package com.atguigu.day09;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Flink09_UDATF_TableAggFun {
    public static void main(String[] args) throws Exception {
        //1.创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<WaterSensor> waterSensorStream = env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));

        //将流转为动态表
        Table table = tableEnv.fromDataStream(waterSensorStream);

        //不注册函数直接使用
//        table.groupBy($("id"))
//                .flatAggregate(call(Top2.class,$("vc")))
//                .select($("id"),$("f0").as("value"),$("f1").as("paiMing")).execute().print();

        //先注册再使用
        tableEnv.createTemporarySystemFunction("Top2", Top2.class);

        //tableAPI
        table.groupBy($("id"))
                .flatAggregate(call("Top2",$("vc")))
                .select($("id"),$("f0").as("value"),$("f1").as("paiMing")).execute().print();

        //无法使用SQL写法

    }

    //自定义一个累加器
    public static class vcTop2 {
        public Integer first = Integer.MIN_VALUE;
        public Integer second = Integer.MIN_VALUE;


    }

    public static class Top2 extends TableAggregateFunction<Tuple2<Integer,Integer>,vcTop2>{

        //创建累加器
        @Override
        public vcTop2 createAccumulator() {
            return new vcTop2();
        }

        //比较数据，如果当前数据大于累加器中存在的数据则替换，并将累加器啊中的数据往下赋值
        public void accumulate(vcTop2 acc, Integer value) {
            if (value > acc.first) {
                acc.second = acc.first;
                acc.first=value;
            }else if(value>acc.second) {
                acc.second=value;
            }
        }

        //计算（排名）
        public void emitValue(vcTop2 acc, Collector<Tuple2<Integer, Integer>> out) {
            if (acc.first != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.first,1));
            }
            if (acc.second != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.second,2));
            }
        }

    }
}
