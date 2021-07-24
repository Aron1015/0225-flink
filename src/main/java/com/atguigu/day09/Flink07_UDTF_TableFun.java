package com.atguigu.day09;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Flink07_UDTF_TableFun {
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
//        table.joinLateral(call(SplitFun.class, $("id")))
//                .select($("id"),$("word")).execute().print();

        //先注册再使用
        tableEnv.createTemporarySystemFunction("split", SplitFun.class);

        //tableAPI
//        table.joinLateral(call("split", $("id")))
//                .select($("id"),$("word")).execute().print();

        //SQL
        tableEnv.executeSql("select id, word from "+table+", lateral table(split(id))").print();
    }
    //hint暗示，主要作用为类型推断时使用
    @FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
    public static class SplitFun extends TableFunction<Row>{
        public void eval(String value) {
            for (String s : value.split("_")) {
                collect(Row.of(s));
            }
        }
    }
}
