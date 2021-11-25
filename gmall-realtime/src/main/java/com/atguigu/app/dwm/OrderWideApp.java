package com.atguigu.app.dwm;


import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.OrderDetail;
import com.atguigu.bean.OrderInfo;
import com.atguigu.bean.OrderWide;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

public class OrderWideApp {
    public static void main(String[] args) {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //1.1 设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/dwd_log/ck"));
//        //1.2 开启CK
//        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000L);

        //TODO 2.读取Kafka订单和订单明细主题数据 dwd_order_info  dwd_order_detail
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";

        DataStreamSource<String> orderInfoKafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId));
        DataStreamSource<String> orderDetailKafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId));

        //TODO 3.将每行数据转换为JavaBean,提取时间戳生成WaterMark
        WatermarkStrategy<OrderInfo> orderInfoWatermarkStrategy = WatermarkStrategy.<OrderInfo>forMonotonousTimestamps().withTimestampAssigner((elem, ts) -> elem.getCreate_ts());
        WatermarkStrategy<OrderDetail> orderDetailWatermarkStrategy = WatermarkStrategy.<OrderDetail>forMonotonousTimestamps().withTimestampAssigner((elem, ts) -> elem.getCreate_ts());

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        KeyedStream<OrderInfo, Long> orderInfoWithIdKeyedStream = orderInfoKafkaDS.map(json -> {

                    // 将JSON字符串转换为Javabean
                    OrderInfo orderInfo = JSONObject.parseObject(json, OrderInfo.class);
                    // 取出时间字段 2021-11-20 21:04:08
                    String create_time = orderInfo.getCreate_time();
                    // 按照空格分割
                    String[] createTimeArr = create_time.split(" ");

                    orderInfo.setCreate_date(createTimeArr[0]);
                    orderInfo.setCreate_hour(createTimeArr[1]);
                    orderInfo.setCreate_ts(sdf.parse(create_time).getTime());

                    return orderInfo;
                })
                .assignTimestampsAndWatermarks(orderInfoWatermarkStrategy)
                .keyBy(json -> json.getId());

        KeyedStream<OrderDetail, Long> orderDetailWithOrderIdKeyedStream = orderDetailKafkaDS.map(json -> {
                    OrderDetail orderDetail = JSONObject.parseObject(json, OrderDetail.class);
                    orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time()).getTime());
                    return orderDetail;
                })
                .assignTimestampsAndWatermarks(orderDetailWatermarkStrategy)
                .keyBy(json -> json.getOrder_id());

        //TODO 4.双流JOIN
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoWithIdKeyedStream
                .intervalJoin(orderDetailWithOrderIdKeyedStream)
                // 生产环境,为了不丢数据,设置时间为最大网络延迟
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>.Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(orderInfo,orderDetail));
                    }
                });


    }
}
