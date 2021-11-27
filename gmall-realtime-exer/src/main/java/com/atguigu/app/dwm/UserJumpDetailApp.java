package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;


public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        // 读取dwd_page_log数据，创建流
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer("dwd_page_log", "UserJumpDetailApp"));

        // 将流转为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.map(JSON::parseObject);

        // 抽取时间字段，添加水印
        SingleOutputStreamOperator<JSONObject> jsonWithWMDS = jsonDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        }));

        // 按照Mid，keyby分组
        KeyedStream<JSONObject, String> keyedStream = jsonWithWMDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        // 构建模式
        Pattern<JSONObject, JSONObject> pattern = Pattern
                .<JSONObject>begin("start")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getJSONObject("page").getString("last_page_id") == null;
                    }
                })
                .times(2)
                .consecutive()
                .within(Time.seconds(10));

        // 应用模式
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        // 提取事件
        OutputTag<JSONObject> overTimeTag = new OutputTag<JSONObject>("OverTimeTag") {
        };
        SingleOutputStreamOperator<JSONObject> selectDS = patternStream.select(
                overTimeTag,
                new PatternTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp) throws Exception {
                        return pattern.get("start").get(0);
                    }
                },
                new PatternSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject select(Map<String, List<JSONObject>> pattern) throws Exception {
                        return pattern.get("start").get(0);
                    }
                }
        );

        DataStream<JSONObject> overTimeDS = selectDS.getSideOutput(overTimeTag);

        // 打印测试
        selectDS.print("selectDS>>>>>");
        overTimeDS.print("overTimeDS>>>>>");


        // 输出到kafka
        DataStream<JSONObject> dataStream = selectDS.union(overTimeDS);
        dataStream.map(json -> JSONObject.toJSONString(json)).addSink(MyKafkaUtil.getFlinkKafkaProducer("dwm_user_jump_detail"));


        // 执行任务
        env.execute();
    }
}
