package com.atguigu.day09;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Flink08_UDAF_AggFun {
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
//                .select($("id"),call(MyAvgFun.class,$("vc")).as("avgVc")).execute().print();

        //先注册再使用
        tableEnv.createTemporarySystemFunction("MyAvgFun", MyAvgFun.class);

        //tableAPI
//        table.groupBy($("id"))
//                .select($("id"),call("MyAvgFun", $("vc")).as("avgVc")).execute().print();

        //SQL
        tableEnv.executeSql("select id, MyAvgFun(vc) from "+table+" group by id").print();

    }

    //自定义一个累加器
    public static class MyAccumulat {
        public Integer vcSum;
        public Integer count;

    }

    public static class MyAvgFun extends AggregateFunction<Double, MyAccumulat> {
        /**
         *累加操作
         * @param acc
         * @param value
         */
        public void accumulate(MyAccumulat acc, Integer value) {
            acc.vcSum+=value;
            acc.count+=1;
        }

        /**
         * 获取最终结果
         *
         * @param accumulator
         * @return
         */
        @Override
        public Double getValue(MyAccumulat accumulator) {
            return accumulator.vcSum*1D/accumulator.count;
        }

        /**
         * 初始化累加器
         *
         * @return
         */
        @Override
        public MyAccumulat createAccumulator() {
            MyAccumulat myAccumulat = new MyAccumulat();
            myAccumulat.count = 0;
            myAccumulat.vcSum = 0;
            return myAccumulat;
        }
    }
}
