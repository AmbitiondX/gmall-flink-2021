package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimAsyncFunction;
import com.atguigu.bean.OrderWide;
import com.atguigu.bean.PaymentWide;
import com.atguigu.bean.ProductStats;
import com.atguigu.common.GmallConstant;
import com.atguigu.utils.ClickhouseUtil;
import com.atguigu.utils.DateTimeUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.text.ParseException;
import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class ProductStatsApp {
    public static void main(String[] args) throws Exception {

        //TODO 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 消费kafka主题，获取流
        String groupId = "product_stats_app_2021";

        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";
        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtil.getFlinkKafkaConsumer(pageViewSourceTopic, groupId);
        FlinkKafkaConsumer<String> orderWideSource = MyKafkaUtil.getFlinkKafkaConsumer(orderWideSourceTopic, groupId);
        FlinkKafkaConsumer<String> paymentWideSource = MyKafkaUtil.getFlinkKafkaConsumer(paymentWideSourceTopic, groupId);
        FlinkKafkaConsumer<String> favorInfoSourceSouce = MyKafkaUtil.getFlinkKafkaConsumer(favorInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> cartInfoSource = MyKafkaUtil.getFlinkKafkaConsumer(cartInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> refundInfoSource = MyKafkaUtil.getFlinkKafkaConsumer(refundInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> commentInfoSource = MyKafkaUtil.getFlinkKafkaConsumer(commentInfoSourceTopic, groupId);

        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);
        DataStreamSource<String> favorInfoDStream = env.addSource(favorInfoSourceSouce);
        DataStreamSource<String> orderWideDStream = env.addSource(orderWideSource);
        DataStreamSource<String> paymentWideDStream = env.addSource(paymentWideSource);
        DataStreamSource<String> cartInfoDStream = env.addSource(cartInfoSource);
        DataStreamSource<String> refundInfoDStream = env.addSource(refundInfoSource);
        DataStreamSource<String> commentInfoDStream = env.addSource(commentInfoSource);

        //TODO 将流中的数据转化为javabean

        // 点击和曝光
        SingleOutputStreamOperator<ProductStats> productStatsWithClickAndDisplayDS = pageViewDStream.flatMap(new FlatMapFunction<String, ProductStats>() {
            @Override
            public void flatMap(String value, Collector<ProductStats> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                JSONObject page = jsonObject.getJSONObject("page");

                // 抽取页面数据
                String pageId = page.getString("page_id");
                String itemType = page.getString("item_type");
                Long ts = jsonObject.getLong("ts");

                if ("good_detail".equals(pageId) && "sku_id".equals(itemType)) {
                    Long skuId = page.getLong("item");

                    out.collect(ProductStats.builder()
                            .sku_id(skuId)
                            .click_ct(1L)
                            .ts(ts)
                            .build());
                }

                JSONArray displays = jsonObject.getJSONArray("displays");
                if (displays != null && displays.size() > 0) {

                    for (int i = 0; i < displays.size(); i++) {

                        JSONObject display = displays.getJSONObject(i);
                        long skuId = display.getLong("item");
                        if ("sku_id".equals(display.getString("item_type"))) {
                            out.collect(ProductStats.builder()
                                    .sku_id(skuId)
                                    .display_ct(1L)
                                    .ts(ts)
                                    .build());
                        }
                    }
                }
            }
        });

        // 收藏数据
        SingleOutputStreamOperator<ProductStats> productStatsWithFavoDS = favorInfoDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                return ProductStats.builder()
                        .sku_id(jsonObject.getLong("sku_id"))
                        .favor_ct(1L)
                        .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                        .build();
            }
        });
        
        // 加购数据
        SingleOutputStreamOperator<ProductStats> productStatsWithCartDS = cartInfoDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                return ProductStats.builder()
                        .sku_id(jsonObject.getLong("sku_id"))
                        .cart_ct(1L)
                        .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                        .build();
            }
        });


        // 下单数据 下单商品个数 下单商品金额 订单数
        SingleOutputStreamOperator<ProductStats> productStatsWithOrderDS = orderWideDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {

                OrderWide orderWide = JSONObject.parseObject(value, OrderWide.class);
                Long skuId = orderWide.getSku_id();
                Long skuNum = orderWide.getSku_num();
                BigDecimal totalAmount = orderWide.getSplit_total_amount();
                Long ts = DateTimeUtil.toTs(orderWide.getCreate_time());
                HashSet<Long> orderIdSet = new HashSet<>();
                orderIdSet.add(orderWide.getOrder_id());

                return ProductStats.builder()
                        .sku_id(skuId)
                        .order_sku_num(skuNum)
                        .order_amount(totalAmount)
                        .orderIdSet(orderIdSet)
                        .ts(ts)
                        .build();
            }
        });

        // 支付数据  支付金额 支付订单数
        SingleOutputStreamOperator<ProductStats> productStatsWithPaymentDS = paymentWideDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {

                PaymentWide paymentWide = JSON.parseObject(value, PaymentWide.class);
                HashSet<Long> paymentSet = new HashSet<>();
                paymentSet.add(paymentWide.getOrder_id());

                return ProductStats.builder()
                        .sku_id(paymentWide.getSku_id())
                        .payment_amount(paymentWide.getSplit_total_amount())
                        .paidOrderIdSet(paymentSet)
                        .ts(DateTimeUtil.toTs(paymentWide.getPayment_create_time()))
                        .build();
            }
        });

        // 退单 退款订单数  退款金额
        SingleOutputStreamOperator<ProductStats> productStatsWithRefundDS = refundInfoDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {

                JSONObject jsonObject = JSON.parseObject(value);
                HashSet<Long> refundSet = new HashSet<>();
                refundSet.add(jsonObject.getLong("order_id"));

                return ProductStats.builder()
                        .sku_id(jsonObject.getLong("sku_id"))
                        .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                        .refundOrderIdSet(refundSet)
                        .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                        .build();
            }
        });

        // 评价  评论订单数  好评订单数
        SingleOutputStreamOperator<ProductStats> productStatsWithCommentDS = commentInfoDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {

                JSONObject jsonObject = JSON.parseObject(value);
                String appraise = jsonObject.getString("appraise");
                long goodCt = GmallConstant.APPRAISE_GOOD.equals(appraise) ? 1 : 0;


                return ProductStats.builder()
                        .sku_id(jsonObject.getLong("sku_id"))
                        .comment_ct(1L)
                        .good_comment_ct(goodCt)
                        .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                        .build();
            }
        });


        //TODO union7条流
        DataStream<ProductStats> unionDS = productStatsWithClickAndDisplayDS.union(
                productStatsWithFavoDS,
                productStatsWithCartDS,
                productStatsWithOrderDS,
                productStatsWithPaymentDS,
                productStatsWithRefundDS,
                productStatsWithCommentDS
        );

        //TODO 生成watermark
        SingleOutputStreamOperator<ProductStats> productStatsWithWMDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
            @Override
            public long extractTimestamp(ProductStats element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        //TODO 按照sku_id keyby, 开窗、聚合
        SingleOutputStreamOperator<ProductStats> productStatsDS = productStatsWithWMDS
                .keyBy(ProductStats::getSku_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(
                        new ReduceFunction<ProductStats>() {
                            @Override
                            public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                                stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                                stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                                stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                                stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                                stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                                stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                                //stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
                                stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());
                                stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                                stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                                //stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);
                                stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                                stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                                //stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);

                                stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                                stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());
                                return stats1;
                            }
                        },
                        new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                            @Override
                            public void apply(Long aLong, TimeWindow window, Iterable<ProductStats> input, Collector<ProductStats> out) throws Exception {
                                // 取出数据
                                ProductStats productStats = input.iterator().next();

                                // 补充订单个数
                                productStats.setOrder_ct((long) productStats.getOrderIdSet().size());
                                productStats.setRefund_order_ct((long) productStats.getRefundOrderIdSet().size());
                                productStats.setPaid_order_ct((long) productStats.getPaidOrderIdSet().size());

                                // 补充窗口时间
                                productStats.setStt(DateTimeUtil.toYMDhms(new Date(window.getStart())));
                                productStats.setEdt(DateTimeUtil.toYMDhms(new Date(window.getEnd())));

                                // 输出数据
                                out.collect(productStats);

                            }
                        });

        //TODO 关联维度表

        // 关联商品维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(productStatsDS, new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
            @Override
            public String getKey(ProductStats input) {
                return input.getSku_id().toString();
            }

            @Override
            public void join(ProductStats input, JSONObject dimInfo) throws ParseException {
                input.setSku_name(dimInfo.getString("SKU_NAME"));
                input.setSku_price(dimInfo.getBigDecimal("PRICE"));
                input.setSpu_id(dimInfo.getLong("SPU_ID"));
                input.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
                input.setTm_id(dimInfo.getLong("TM_ID"));

            }
        }, 60, TimeUnit.SECONDS);

        // 关联spu维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDstream =
                AsyncDataStream.unorderedWait(productStatsWithSkuDS,
                        new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws ParseException {
                                productStats.setSpu_name(jsonObject.getString("SPU_NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getSpu_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        // 关联 Category维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3Dstream =
                AsyncDataStream.unorderedWait(productStatsWithSpuDstream,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws ParseException {
                                productStats.setCategory3_name(jsonObject.getString("NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getCategory3_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        // 关联TM维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDstream =
                AsyncDataStream.unorderedWait(productStatsWithCategory3Dstream,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws ParseException {
                                productStats.setTm_name(jsonObject.getString("TM_NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getTm_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        //TODO 将数据写入clickhouse
        productStatsWithTmDstream.print(">>>>>>");

        productStatsWithTmDstream.addSink(ClickhouseUtil.getJdbcSink("insert into product_stats_2021 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));


        //TODO 执行任务
        env.execute("ProductStatsApp");

    }
}
