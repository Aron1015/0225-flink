package com.atguigu.day03.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

public class Flink02_Sink_Redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        FlinkJedisPoolConfig redisConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop102")
                .setPort(6379)
                .build();

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        socketTextStream.process(new ProcessFunction<String, WaterSensor>() {
            @Override
            public void processElement(String value, Context ctx, Collector<WaterSensor> out) throws Exception {
                String[] words = value.split(" ");
                out.collect(new WaterSensor(words[0],Long.parseLong(words[1]),Integer.parseInt(words[2])));
            }
        })
                .addSink(new RedisSink<>(redisConfig, new RedisMapper<WaterSensor>() {
                    /**
                     * 插入数据的命令,当使用两个参数的构造方法时，一般是Hash类型的，第二个参数指定的是Redis的大Key
                     * @return
                     */
                    @Override
                    public RedisCommandDescription getCommandDescription() {
//                        return new RedisCommandDescription(RedisCommand.HSET,System.currentTimeMillis()+"");
                        return new RedisCommandDescription(RedisCommand.SET);
                    }

                    /**
                     * 指定redisKey(当为Hash时，这个key为小key,即filed)，默认情况下是redisKey
                     * @param data
                     * @return
                     */
                    @Override
                    public String getKeyFromData(WaterSensor data) {
                        return data.getId();
                    }

                    /**
                     * 插入的数据
                     * @param data
                     * @return
                     */
                    @Override
                    public String getValueFromData(WaterSensor data) {
                        return JSON.toJSONString(data);
                    }
                }));
                
        
        

        env.execute();
    }
}
