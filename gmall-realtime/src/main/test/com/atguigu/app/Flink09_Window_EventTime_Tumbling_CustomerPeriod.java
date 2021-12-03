package com.atguigu.app;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Timer;
import java.util.TimerTask;


public class Flink09_Window_EventTime_Tumbling_CustomerPeriod {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.读取端口数据并转为javaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("localhost", 9999)
                .map(date -> {
                    String[] words = date.split(" ");
                    return new WaterSensor(words[0], Long.parseLong(words[1]), (double) Integer.parseInt(words[2]));
                });


        //3.自定义WatermarkStrategy
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = waterSensorDS.assignTimestampsAndWatermarks(new WatermarkStrategy<WaterSensor>() {
            //生成自定义的watermark
            @Override
            public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new Test01.Myperiod(2000L);
            }
        }.withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {//提取数据的时间戳
            @Override
            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                return element.getTs() * 1000L;
            }
        }));

        //4.按照id分组
        KeyedStream<WaterSensor, String> keyedStream = waterSensorSingleOutputStreamOperator.keyBy(date -> date.getId());

        //5.开窗
        WindowedStream<WaterSensor, String, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));

        //6.计算总和
        window.sum("vc").print();


        env.execute();
    }


}
