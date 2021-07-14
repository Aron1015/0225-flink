package com.atguigu.day02.source;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class Flink05_Source_Custom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 通过定义数据源获取数据
        env.addSource(new MySource()).print();

        env.execute();
    }

    public static class MySource implements SourceFunction<WaterSensor>{
        private Random random = new Random();
        private Boolean run = true;

        /**
         * 发送数据的方法
         * @param ctx
         * @throws Exception
         */
        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            while (run) {
                ctx.collect(new WaterSensor("sensor" + random.nextInt(10), System.currentTimeMillis(), random.nextInt(10)*100));
                Thread.sleep(1000);
            }
        }

        /**
         * 取消任务，一般不自己调用
         */
        @Override
        public void cancel() {
            run=false;
        }
    }
}
