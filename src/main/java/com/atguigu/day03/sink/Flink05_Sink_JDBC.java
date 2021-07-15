package com.atguigu.day03.sink;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.*;

public class Flink05_Sink_JDBC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] words = value.split(" ");
                return new WaterSensor(words[0], Long.parseLong(words[1]), Integer.parseInt(words[2]));
            }
        });

        waterSensorDS.addSink(JdbcSink.sink("insert into sensor values(?, ?, ?)", new JdbcStatementBuilder<WaterSensor>() {
            @Override
            public void accept(PreparedStatement ps, WaterSensor waterSensor) throws SQLException {
                ps.setString(1, waterSensor.getId());
                ps.setLong(2, waterSensor.getTs());
                ps.setInt(3, waterSensor.getVc());

            }
        },
                //TODO 读取无界流时，指定条数，达到这个数据条数，则把数据写入Mysql
                JdbcExecutionOptions.builder().withBatchSize(1).build(),
                //TODO 读取无界流时，指定间隔时间，达到这个时间，则把数据写入Mysql
//                JdbcExecutionOptions.builder().withBatchIntervalMs(10000).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:mysql://hadoop102:3306/test?useSSL=false")
                .withDriverName("java.sql.Driver")
                .withUsername("root")
                .withPassword("123456")
                .build()));

        env.execute();
    }
}
