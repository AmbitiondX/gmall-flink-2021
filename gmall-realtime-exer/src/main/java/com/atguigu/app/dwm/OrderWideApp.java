package com.atguigu.app.dwm;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimAsyncFunction;
import com.atguigu.bean.OrderDetail;
import com.atguigu.bean.OrderInfo;
import com.atguigu.bean.OrderWide;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

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

                    OrderInfo orderInfo = JSON.parseObject(line, OrderInfo.class);

//                    System.out.println(line);
                    String create_time = orderInfo.getCreate_time();
                    String[] dateTimeArr = create_time.split(" ");
                    orderInfo.setCreate_date(dateTimeArr[0]);
                    orderInfo.setCreate_hour(dateTimeArr[1].split(":")[0]);

                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                    orderInfo.setCreate_ts(sdf.parse(create_time).getTime());

                    return orderInfo;
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }));

        SingleOutputStreamOperator<OrderDetail> orderDetailWithWMDS = orderDetailDS.map(line -> {
//                    System.out.println(line);
                    OrderDetail orderDetail = JSON.parseObject(line, OrderDetail.class);
                    String create_time = orderDetail.getCreate_time();
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    orderDetail.setCreate_ts(sdf.parse(create_time).getTime());
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
        //5.1 关联用户维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(orderWideDS, new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {

            @Override
            public String getKey(OrderWide input) {
                return input.getUser_id().toString();
            }

            @Override
            public void join(OrderWide input, JSONObject dimInfo) throws ParseException {
                input.setUser_gender(dimInfo.getString("GENDER"));
                String birthday = dimInfo.getString("BIRTHDAY");
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

                long birthTs = sdf.parse(birthday).getTime();
                long age = (System.currentTimeMillis() - birthTs) / 1000 / 60 / 60 / 24 / 365;
                input.setUser_age((int) age);

            }
        }, 60, TimeUnit.SECONDS);

//        orderWideWithUserDS.print("User>>>>>>>>>>");

        //5.2 关联地区维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserProvince = AsyncDataStream.unorderedWait(orderWideWithUserDS, new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
            @Override
            public String getKey(OrderWide input) {
                return input.getProvince_id().toString();
            }

            @Override
            public void join(OrderWide input, JSONObject dimInfo) throws ParseException {
                input.setProvince_name(dimInfo.getString("NAME"));
                input.setProvince_iso_code("ISO_CODE");
                input.setProvince_area_code("AREA_CODE");
                input.setProvince_3166_2_code("ISO_3166_2");
            }
        }, 60, TimeUnit.SECONDS);

        //5.3 关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSku = AsyncDataStream.unorderedWait(orderWideWithUserProvince, new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
            @Override
            public String getKey(OrderWide input) {
                return input.getSku_id().toString();
            }

            @Override
            public void join(OrderWide input, JSONObject dimInfo) throws ParseException {
                input.setSku_name(dimInfo.getString("SKU_NAME"));
                input.setSpu_id(dimInfo.getLong("SPU_ID"));
                input.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
                input.setTm_id(dimInfo.getLong("TM_ID"));
            }
        }, 60, TimeUnit.SECONDS);

        //5.4 关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpu = AsyncDataStream.unorderedWait(orderWideWithSku, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
            @Override
            public String getKey(OrderWide input) {
                return input.getSpu_id().toString();
            }

            @Override
            public void join(OrderWide input, JSONObject dimInfo) throws ParseException {
                input.setSpu_name(dimInfo.getString("SPU_NAME"));
            }
        }, 60, TimeUnit.SECONDS);

        //5.5 关联品类维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3 = AsyncDataStream.unorderedWait(orderWideWithSpu, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
            @Override
            public String getKey(OrderWide input) {
                return input.getCategory3_id().toString();
            }

            @Override
            public void join(OrderWide input, JSONObject dimInfo) throws ParseException {
                input.setCategory3_name(dimInfo.getString("NAME"));
            }
        }, 60, TimeUnit.SECONDS);

        //5.6 关联TM维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTM = AsyncDataStream.unorderedWait(orderWideWithCategory3, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
            @Override
            public String getKey(OrderWide input) {
                return input.getTm_id().toString();
            }

            @Override
            public void join(OrderWide input, JSONObject dimInfo) throws ParseException {
                input.setTm_name(dimInfo.getString("TM_NAME"));
            }
        }, 60, TimeUnit.SECONDS);

        // 输出流到kafka
        orderWideWithTM.print("orderWideWithTM>>>");
        orderWideWithTM.map(orderWide -> JSON.toJSONString(orderWide)).addSink(MyKafkaUtil.getFlinkKafkaProducer("dwm_order_wide"));

        // 执行任务
        env.execute("OrderWideApp");
    }
}
