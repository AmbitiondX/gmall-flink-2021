package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimAsyncFunction;
import com.atguigu.bean.OrderInfo;
import com.atguigu.bean.OrderWide;
import com.atguigu.bean.PaymentInfo;
import com.atguigu.bean.PaymentWide;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        // 获取kafka中的流
        String groupId = "PaymentWideApp";
        DataStreamSource<String> paymentInfoDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer("dwd_payment_info", groupId));
        DataStreamSource<String> orderWideDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer("dwm_order_wide", groupId));

        // 将流转换为javaBean，补全字段，并生成水印
        SingleOutputStreamOperator<PaymentInfo> paymentInfoWithWMDS = paymentInfoDS.map(line -> {
                    PaymentInfo paymentInfo = JSONObject.parseObject(line, PaymentInfo.class);
                    return paymentInfo;
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                            @Override
                            public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return sdf.parse(element.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    return  recordTimestamp;
                                }
                            }
                        }));

        SingleOutputStreamOperator<OrderWide> orderWideWithWMDS = orderWideDS.map(line -> {
                    OrderWide orderWide = JSONObject.parseObject(line, OrderWide.class);
                    return orderWide;
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                            @Override
                            public long extractTimestamp(OrderWide element, long recordTimestamp) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return sdf.parse(element.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    return recordTimestamp;
                                }

                            }
                        }));


        // 双流join
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoWithWMDS.keyBy(PaymentInfo::getOrder_id)
                .intervalJoin(orderWideWithWMDS.keyBy(OrderWide::getOrder_id))
                .between(Time.minutes(-15), Time.seconds(0))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>.Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });

        // 打印流
        SingleOutputStreamOperator<String> jsonStrDS = paymentWideDS.map(line -> JSONObject.toJSONString(line));
        jsonStrDS.print("paymentWideDS>>>");

        jsonStrDS.addSink(MyKafkaUtil.getFlinkKafkaProducer("dwm_payment_wide"));


        // 启动任务
        env.execute("PaymentWideApp");

    }
}
