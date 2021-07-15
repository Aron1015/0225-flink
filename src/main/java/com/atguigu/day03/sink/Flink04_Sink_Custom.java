package com.atguigu.day03.sink;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class Flink04_Sink_Custom {
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

        waterSensorDS.addSink(new RichSinkFunction<WaterSensor>() {
            private Connection connection;
            private PreparedStatement ps;

            @Override
            public void invoke(WaterSensor value, Context context) throws Exception {
                ps.setString(1,value.getId());
                ps.setLong(2, value.getTs());
                ps.setInt(3, value.getVc());
                //真正执行语句
                ps.execute();
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                //获取连接
                connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test?useSSL=false", "root", "123456");
                //語句预执行者
                ps = connection.prepareStatement("insert into sensor values(?, ?, ?)");
            }

            @Override
            public void close() throws Exception {
                connection.close();
                ps.close();
            }
        });


        env.execute();
    }
}
