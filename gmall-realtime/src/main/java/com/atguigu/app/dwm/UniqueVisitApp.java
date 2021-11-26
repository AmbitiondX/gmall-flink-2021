package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Date;

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
        String groupId = "unique_visit_app_1109";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";

        FlinkKafkaConsumer<String> flinkKafkaConsumer = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(flinkKafkaConsumer);

        //TODO 3.将数据转为JSON对象
        OutputTag<String> dirtyData = new OutputTag<String>("dirtyData"){};
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirtyData, value);
                }
            }
        });

        //TODO 4.按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //TODO 5.使用状态编程的方式过滤数据
        // 过滤掉不是今天第一次访问的数据
        SingleOutputStreamOperator<JSONObject> filterDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> firstVisitTime;
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("visit-state", String.class);
                sdf = new SimpleDateFormat("yyyy-MM-DD");

                //创建状态TTL配置项
                StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                valueStateDescriptor.enableTimeToLive(stateTtlConfig);
                firstVisitTime = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                // 取出上一次访问的页面
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                if (lastPageId == null || lastPageId.length() <= 0) {
                    // 取出状态数据
                    String firstVisitDate = firstVisitTime.value();

                    // 取出数据时间
                    Long ts = value.getLong("ts");
                    String curDate = sdf.format(ts);

                    // 进来的数据都是今天的数据，
                    // 如果数据中的firstVisitDate == null，说明：要么时是用户，要么距离上一次访问已经超过一天了
                    // !firstVisitDate.equals(curDate)，可能是昨天还没过期的数据，所以相等的时候，代表今天已经访问过，取反表示今天还没访问过
                    if (firstVisitDate == null || !firstVisitDate.equals(curDate)) {
                        firstVisitTime.update(curDate);
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        });

        //TODO 6.打印同时写入kafka
        filterDS.print("filterDS>>>>>");
        filterDS.map(JSON::toString).addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        //TODO 7.启动
        env.execute();
    }
}
