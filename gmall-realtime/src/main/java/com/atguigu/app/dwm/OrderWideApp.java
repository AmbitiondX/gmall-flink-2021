package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimAsyncFunction;
import com.atguigu.bean.OrderDetail;
import com.atguigu.bean.OrderInfo;
import com.atguigu.bean.OrderWide;
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
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.1 开启CK
        //        env.enableCheckpointing(5000L);
        //        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //        //正常Cancel任务时,保留最后一次CK
        //        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //        //重启策略
        //        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));
        //        //状态后端
        //        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall-flink-201109/ck"));
        //        //设置访问HDFS的用户名
        //        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 2.读取Kafka dwd_order_info、dwd_order_detail 主题的数据
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "dwm_wide_group_0625";
        DataStreamSource<String> orderInfoStrDS = env.addSource(MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId));
        DataStreamSource<String> orderDetailStrDS = env.addSource(MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId));

        //TODO 3.将数据转换为JavaBean并提取时间戳生成WaterMark
        SingleOutputStreamOperator<OrderInfo> orderInfoWithWMDS = orderInfoStrDS.map(line -> {
                    OrderInfo orderInfo = JSONObject.parseObject(line, OrderInfo.class);

                    // yyyy-MM-dd HH:mm:ss
                    String create_time = orderInfo.getCreate_time();
                    String[] dateHourArr = create_time.split(" ");
                    orderInfo.setCreate_date(dateHourArr[0]);
                    orderInfo.setCreate_hour(dateHourArr[1].split(":")[0]);

                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    long ts = sdf.parse(create_time).getTime();
                    orderInfo.setCreate_ts(ts);
                    return orderInfo;
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }));

        SingleOutputStreamOperator<OrderDetail> orderDetailWihtWMDS = orderDetailStrDS.map(line -> {
                    OrderDetail orderDetail = JSONObject.parseObject(line, OrderDetail.class);

                    String create_time = orderDetail.getCreate_time();
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    long ts = sdf.parse(create_time).getTime();
                    orderDetail.setCreate_ts(ts);
                    return orderDetail;
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }));

        //TODO 4.将两条流进行JOIN
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoWithWMDS.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailWihtWMDS.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>.Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });

        // 打印测试
        orderWideDS.print("OrderWide>>>>>>");

        //TODO 5.关联维度信息
        //TODO 5.1 关联用维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(orderWideDS,
                new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getId(OrderWide input) {
                        return input.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide input, JSONObject dimInfo) {

                        String birthday = dimInfo.getString("BIRTHDAY");
                        String gender = dimInfo.getString("GENDER");

                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

                        // 获取当前时间
                        long ts = System.currentTimeMillis();
                        long time = ts;

                        try {
                            time = sdf.parse(birthday).getTime();
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }

                        // 当前时间减去出生日期，计算年龄
                        long age = (ts - time) / (1000L * 60 * 60 * 34 * 365);

                        // 将写别与年龄补充道OrderWide
                        input.setUser_gender(gender);
                        input.setUser_age(Integer.parseInt(age + ""));
                    }
                }, 100, TimeUnit.SECONDS);

//        orderWideWithUserDS.print("User>>>>>>>");


        //TODO 5.2关联地区维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(orderWideWithUserDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getId(OrderWide input) {
                        return input.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) {
                        String name = dimInfo.getString("NAME");
                        String area_code = dimInfo.getString("AREA_CODE");
                        String iso_code = dimInfo.getString("ISO_CODE");
                        String iso_3166_2 = dimInfo.getString("ISO_3166_2");

                        orderWide.setProvince_name(name);
                        orderWide.setProvince_area_code(area_code);
                        orderWide.setProvince_iso_code(iso_code);
                        orderWide.setProvince_3166_2_code(iso_3166_2);
                    }
                }, 100, TimeUnit.SECONDS);

        //TODO 5.3关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(orderWideWithProvinceDS,
                new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public String getId(OrderWide input) {
                        return input.getSku_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) {
                        orderWide.setSku_name(dimInfo.getString("SKU_NAME"));
                        orderWide.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
                        orderWide.setSpu_id(dimInfo.getLong("SPU_ID"));
                        orderWide.setTm_id(dimInfo.getLong("TM_ID"));

                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 5.4 关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(orderWideWithSkuDS,
                new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public String getId(OrderWide input) {
                        return input.getSpu_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) {
                        orderWide.setSpu_name(dimInfo.getString("SPU_NAME"));
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 5.5关联TradeMark维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(orderWideWithSpuDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getId(OrderWide input) {
                        return input.getTm_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) {
                        orderWide.setTm_name(dimInfo.getString("TM_NAME"));
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 5.6关联Category维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(orderWideWithTmDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getId(OrderWide input) {
                        return input.getCategory3_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) {
                        orderWide.setCategory3_name(dimInfo.getString("NAME"));
                    }
                }, 60, TimeUnit.SECONDS);

        // 打印测试
        orderWideWithCategory3DS.map(JSONObject::toJSONString).print();

        //TODO 6.将数据写入Kafka
        orderWideWithCategory3DS
                .map(JSONObject::toJSONString)
                .addSink(MyKafkaUtil.getKafkaSink(orderWideSinkTopic));

        //TODO 7.启动
        env.execute();
    }
}
