package com.atguigu.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.VisitorStats;
import com.atguigu.utils.ClickhouseUtil;
import com.atguigu.utils.DateTimeUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;
import java.util.Iterator;

public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {

        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        // 从dwd_page_log dwm_user_jump_detail dwm_unique_visit 获取流
        String groupId = "VisitorStatsApp";
        DataStreamSource<String> pageLogDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer("dwd_page_log", groupId));
        DataStreamSource<String> jumpDetailDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer("dwm_user_jump_detail", groupId));
        DataStreamSource<String> uvDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer("dwm_unique_visit", groupId));

//        pageLogDS.print("pageLogDS");
//        jumpDetailDS.print("jumpDetailDS");
//        uvDS.print("uvDS");


        // 将流中的数据转化为Javabean
        SingleOutputStreamOperator<VisitorStats> pageVSDS = pageLogDS.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            JSONObject page = jsonObject.getJSONObject("page");
            long enterCount = page.getString("last_page_id") == null ? 1 : 0;

            return new VisitorStats(
                    "",
                    "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L,
                    1L,
                    enterCount,
                    0L,
                    page.getLong("during_time"),
                    jsonObject.getLong("ts")
            );
        });

        SingleOutputStreamOperator<VisitorStats> jumpDetailVSDS = jumpDetailDS.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            JSONObject page = jsonObject.getJSONObject("page");

            return new VisitorStats(
                    "",
                    "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L,
                    0L,
                    0L,
                    1L,
                    0L,
                    jsonObject.getLong("ts")
            );
        });

        SingleOutputStreamOperator<VisitorStats> uvVSDS = uvDS.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            JSONObject page = jsonObject.getJSONObject("page");

            return new VisitorStats(
                    "",
                    "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L,
                    0L,
                    0L,
                    0L,
                    0L,
                    jsonObject.getLong("ts")
            );
        });

        // 将三条流union到一起
        DataStream<VisitorStats> unionDS = pageVSDS.union(jumpDetailVSDS, uvVSDS);

//        unionDS.print("unionDS");

        SingleOutputStreamOperator<VisitorStats> unionWithWMDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(13))
                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        // keyby
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowDS = unionWithWMDS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                return Tuple4.of(value.getAr(), value.getCh(), value.getVc(), value.getIs_new());
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10)));





        // 开窗计算
        SingleOutputStreamOperator<VisitorStats> reduceDS = windowDS.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());
                value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());
                return value1;
            }
        }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {
                Iterator<VisitorStats> iterator = input.iterator();
                VisitorStats visitorStats = iterator.next();
                visitorStats.setStt(DateTimeUtil.toYMDhms(new Date(window.getStart())));
                visitorStats.setEdt(DateTimeUtil.toYMDhms(new Date(window.getEnd())));
                out.collect(visitorStats);
            }
        });

        // 输出到clickhouse
        reduceDS.print("reduceDS>>>>>");

        reduceDS.addSink(ClickhouseUtil.getJdbcSink("insert into  visitor_stats_2021 values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"));



        // 执行任务
        env.execute("VisitorStatsApp");
    }
}
