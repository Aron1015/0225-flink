package com.atguigu.day09;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink02_SQL_GroupWindow_Tumble {
    public static void main(String[] args) throws Exception {
        //1.创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.创建表的环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //将默认时区从格林威治时区改为东八区
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.local-time-zone", "GMT");

        //TODO 2.1在创建表时,并指定事件时间
        tableEnv.executeSql("create table sensor(" +
                "id string," +
                "ts bigint," +
                "vc int, " +
                "t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                "watermark for t as t - interval '5' second)" +
                "with("
                + "'connector' = 'filesystem',"
                + "'path' = 'input/sensor-sql.txt',"
                + "'format' = 'csv'"
                + ")"
        );

        //3.查询表,并使用滚动窗口
        tableEnv.executeSql("select \n" +
                "id, \n" +
                "tumble_start(t,interval '2' second) as wStart, \n" +
                "tumble_end(t,interval '2' second) as wEnd, \n" +
                "sum(vc) sumVc \n" +
                "from sensor \n" +
                "group by tumble(t,interval '2' second),id").print();

    }
}
