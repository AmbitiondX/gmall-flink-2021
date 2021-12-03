package com.atguigu.app;

import com.atguigu.bean.Bean1;
import com.atguigu.bean.WaterSensor;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.util.Timer;
import java.util.TimerTask;

public class Test01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("localhost", 7777);

        SingleOutputStreamOperator<WaterSensor> beanDS = source.map(line -> {
            String[] split = line.split(" ");
            return new WaterSensor(split[0], Long.parseLong(split[1]), (double) Long.parseLong(split[2]));
        });

        SingleOutputStreamOperator<WaterSensor> beanWithWMDS = beanDS.assignTimestampsAndWatermarks(new WatermarkStrategy<WaterSensor>() {
            //生成自定义的watermark
            @Override
            public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new Myperiod(0L);
            }
        }.withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {//提取数据的时间戳
            @Override
            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                return element.getTs() * 1000L;
            }
        }));

        SingleOutputStreamOperator<WaterSensor> reduceDS = beanWithWMDS
                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                        value1.setVc(value1.getVc() + value2.getVc());
                        return value1;
                    }
                });

        reduceDS.print();

        env.execute();
    }

    //自定义周期性的Watermark生成器
    public static class Myperiod implements WatermarkGenerator<WaterSensor>{


        //最大时间戳
        private Long maxTs;

        //最大延迟时间
        private Long maxDelay;

//        private boolean flag = false;

        //构造方法
        public Myperiod(Long maxDelay) {
            this.maxDelay = maxDelay;
            this.maxTs = Long.MIN_VALUE + this.maxDelay + 1;
        }

        //当数据来的时候调用
        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            maxTs = Math.max(eventTimestamp, maxTs);

            new Timer().schedule(new TimerTask() {
                @Override
                public void run() {
                    maxTs = maxTs + 200;
                }
            },200,200);


            System.out.println("maxTs:" + maxTs);
        }

        //周期性调用
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {

//            System.out.println("生成Watermark");

            System.out.println("maxTs:" + maxTs);
            output.emitWatermark(new Watermark(maxTs-maxDelay-1L));

        }
    }
}
