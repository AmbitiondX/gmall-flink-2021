package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
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
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class UserJumpDetailApp {
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

        //TODO 2.读取Kafka dwd_page_log 主题数据
        String sourceTopic = "dwd_page_log";
        String groupId = "user_jump_detail_app";
        String sinkTopic = "dwm_user_jump_detail";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(sourceTopic, groupId));
        //DataStreamSource<String> kafkaDS = env.socketTextStream("hadoop102", 9999);

        //TODO 3.转换为JSON对象并提取数据中的时间戳生成WaterMark
        OutputTag<String> outputTag = new OutputTag<String>("dirtyData") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);
                            out.collect(jsonObject);
                        } catch (Exception e) {
                            ctx.output(outputTag, value);
                        }
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                }));

        //TODO 4.按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });

        //TODO 5.定义模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("begin")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null || lastPageId.length() <= 0;
                    }
                })
                .times(2) //默认的使用宽松近邻
                .consecutive()
                .within(Time.seconds(10));//手动指定使用严格近邻

        //TODO 6.将模式序列作用在流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        //TODO 7.提取事件(包含超时事件)
        OutputTag<JSONObject> timeOutTag = new OutputTag<JSONObject>("TimeOut") {
        };
        SingleOutputStreamOperator<JSONObject> selectDS = patternStream.select(
                // 匹配上的数据和超时数据都是满足的数据，做相同的处理
                timeOutTag,
                new PatternTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp) throws Exception {
                        return pattern.get("begin").get(0);
                    }
                },
                new PatternSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject select(Map<String, List<JSONObject>> pattern) throws Exception {
                        return pattern.get("begin").get(0);
                    }
                }
        );
        //TODO 8.合并主流(匹配上的事件)和侧输出流(超时事件)
        selectDS.print("selectDS>>>>>>");
        DataStream<JSONObject> timeOutDS = selectDS.getSideOutput(timeOutTag);
        timeOutDS.print("timeOutDS>>>>>>");

        //TODO 9.写入Kafka
        DataStream<JSONObject> reusltDS = selectDS.union(timeOutDS);
        reusltDS.map(json -> json.toJSONString()).addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        //TODO 10.启动
        env.execute();
    }
}
