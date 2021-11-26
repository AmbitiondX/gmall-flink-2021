package com.atguigu.app.dwm;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimAsyncFunction;
import com.atguigu.bean.OrderDetail;
import com.atguigu.bean.OrderInfo;
import com.atguigu.bean.OrderWide;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

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

        //TODO 5.关联维度
        // 5.1关联用户维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(
                orderWideDS,
                new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getId(OrderWide input) {
                        return input.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide input, JSONObject dimInfo) throws Exception {

                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

                        // 取出用户维度中的生日
                        String birthday = dimInfo.getString("BIRTHDAY");
                        long currentTS = System.currentTimeMillis();
                        long ts = sdf.parse(birthday).getTime();

                        // 将生日自选转换为年龄
                        Long ageLong = (currentTS - ts) / 365 / 24 / 60 / 60 / 1000;

                        input.setUser_age(ageLong.intValue());

                        // 取出用户维度中的性别
                        String gender = dimInfo.getString("GENDER");
                        input.setUser_gender(gender);
                    }
                }, 60, TimeUnit.SECONDS);

        // 5.2关联地区维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(
                orderWideWithUserDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getId(OrderWide input) {
                        return input.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide input, JSONObject dimInfo) throws Exception {
                        input.setProvince_name(dimInfo.getString("NAME"));
                        input.setProvince_area_code(dimInfo.getString("AREA_CODE"));
                        input.setProvince_iso_code(dimInfo.getString("ISO_CODE"));
                        input.setProvince_3166_2_code(dimInfo.getString("ISO_3166_2"));
                    }
                }
                , 60, TimeUnit.SECONDS);

        // 5.3 关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(
                orderWideWithProvinceDS, new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setSku_name(jsonObject.getString("SKU_NAME"));
                        orderWide.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                        orderWide.setSpu_id(jsonObject.getLong("SPU_ID"));
                        orderWide.setTm_id(jsonObject.getLong("TM_ID"));
                    }

                    @Override
                    public String getId(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSku_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //5.4 关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(
                orderWideWithSkuDS, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
                    }

                    @Override
                    public String getId(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSpu_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //5.5 关联品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
                orderWideWithSpuDS, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }

                    @Override
                    public String getId(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //5.6 关联品类维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideWithTmDS, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setCategory3_name(jsonObject.getString("NAME"));
                    }

                    @Override
                    public String getId(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }
                }, 60, TimeUnit.SECONDS);

        orderWideWithCategory3DS.print();

        //TODO 6.写入数据到Kafka  dwm_order_wide
        orderWideWithCategory3DS.map(JSONObject::toJSONString).addSink(MyKafkaUtil.getKafkaSink(orderWideSinkTopic));

    }
}
