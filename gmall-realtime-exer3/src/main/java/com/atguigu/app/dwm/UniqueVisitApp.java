package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;

public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        // 1.1 开启ck
        /*env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 正常cancel任务时，保留最后一次CK
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));
        // 状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/dwm_log/ck"));
        //设置访问HDFS的用户名
        System.setProperty("HADOOP_USER_NAME", "atguigu");*/

        //TODO 2.读取kafka dwd_page_log 主题数据
        String groupId = "unique_visit_app";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";

        FlinkKafkaConsumer<String> flinkKafkaConsumer = MyKafkaUtil.getFlinkKafkaConsumer(sourceTopic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(flinkKafkaConsumer);

        // 将数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        // keyby聚合
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });

        // 过滤数据，逻辑处理
        SingleOutputStreamOperator<JSONObject> filterDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> firstvisitDt;
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {

                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("first-visit", String.class);

                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                valueStateDescriptor.enableTimeToLive(ttlConfig);

                firstvisitDt = getRuntimeContext().getState(valueStateDescriptor);

                sdf = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {

                String lastPageId = value.getJSONObject("page").getString("last_page_id");

                if (lastPageId == null) {

                    String visitDt = firstvisitDt.value();
                    String curDt = sdf.format(value.getLong("ts"));

                    if (visitDt == null || !(curDt.equals(visitDt))) {
                        firstvisitDt.update(curDt);
                        return true;
                    }

                }

                return false;
            }
        });


        // 将filterDS写到Kafka
        filterDS.map(JSON::toString).addSink(MyKafkaUtil.getFlinkKafkaProducer(sinkTopic));

        // 启动任务
        env.execute();
    }
}
