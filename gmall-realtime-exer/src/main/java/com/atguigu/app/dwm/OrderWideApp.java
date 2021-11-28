package com.atguigu.app.dwm;


import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.OrderDetail;
import com.atguigu.bean.OrderInfo;
import com.atguigu.bean.OrderWide;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        // 读取Kafka dwd_order_info dwd_order_detail主题数据，转化为流
        String groupId = "OrderWideApp";
        DataStreamSource<String> orderInfoDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer("dwd_order_info",groupId));
        DataStreamSource<String> orderDetailDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer("dwd_order_detail",groupId));

        // 将流转化为JavaBean，补全字，并生成watermark
        SingleOutputStreamOperator<OrderInfo> orderInfoWithWMDS = orderInfoDS.map(line -> {
                    OrderInfo orderInfo = JSONObject.parseObject(line, OrderInfo.class);
                    String[] createTime = orderInfo.getCreate_time().split(" ");
                    orderInfo.setCreate_date(createTime[0]);
                    orderInfo.setCreate_hour(createTime[1].split(":")[0]);
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    long ts = sdf.parse(orderInfo.getCreate_time()).getTime();
                    orderInfo.setCreate_ts(ts);
                    return orderInfo;
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }));

        SingleOutputStreamOperator<OrderDetail> orderDetailWithWMDS = orderDetailDS.map(line -> {
                    OrderDetail orderDetail = JSONObject.parseObject(line, OrderDetail.class);
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    long ts = sdf.parse(orderDetail.getCreate_time()).getTime();
                    orderDetail.setCreate_ts(ts);
                    return orderDetail;
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }));

        // IntervalJoin 两条流，生成宽表流
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoWithWMDS.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailWithWMDS.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>.Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });

        // 关联维度表，补全JavaBean属性
        orderWideDS.map(orderWide -> {
            Long userId = orderWide.getUser_id();
//            querySQL(connection,sql,false)

            return orderWide;
        });

        // 输出流到kafka

        // 执行任务
        env.execute("OrderWideApp");
    }
}
