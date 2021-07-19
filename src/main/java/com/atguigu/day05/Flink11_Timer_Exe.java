package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class Flink11_Timer_Exe {
    public static void main(String[] args) throws Exception {
        //1.流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3.对数据进行处理，封装成WaterSensor
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(" ");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //监控水位传感器的水位值，如果水位值在五秒钟之内(processing time)连续上升，则报警，并将报警信息输出到侧输出流。

        SingleOutputStreamOperator<String> result = waterSensorDStream.keyBy("id")
                .process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {
                    //用来记录上一次水位高度
                    private Integer lastVc = Integer.MIN_VALUE;
                    //用来记录定时器时间
                    private Long timerTs = Long.MIN_VALUE;

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        //判断当前水位线是否高于上次水位
                        if (value.getVc() > lastVc) {
                            //判断定时器是否重置，是否为第一条数据
                            if (timerTs == Long.MIN_VALUE) {
                                System.out.println("注册定时器。。。。");
                                //注册5s定时器
                                timerTs = ctx.timerService().currentProcessingTime() + 5000;
                                ctx.timerService().registerProcessingTimeTimer(timerTs);
                            }
                        } else {
                            //如果水位线没有上升则删除定时器
                            ctx.timerService().deleteProcessingTimeTimer(timerTs);
                            System.out.println("删除定时器");
                            //将定时器时间重置
                            timerTs = Long.MIN_VALUE;
                        }
                        //最后更新水位线以便下次对比
                        lastVc = value.getVc();
                        out.collect(value.toString());
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        ctx.output(new OutputTag<String>("output"){}, ctx.getCurrentKey()+"报警！！！");
                        //重置定时器
                        timerTs=Long.MIN_VALUE;
                    }
                });

        result.print("主线");
        result.getSideOutput(new OutputTag<String>("output"){}).print("副线");


        env.execute();
    }
}
