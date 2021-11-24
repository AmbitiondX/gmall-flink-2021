package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

public class BaseLogApp {
    //流向    前端埋点 -> nginx -> 日志服务器 -> kafka(zk) -> flinkApp -> kafka
    //进程    mock    -> nginx -> logger.sh -> kafka(zk) -> flinkApp -> kafka
    public static void main(String[] args) throws Exception {
        // 1.获取流的执行环境，设置并行度，开启CK，设置状态后端(hdfs)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 为kafka主题的分区数
        env.setParallelism(1);
        /*// 设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/dwd_log/ck"));
        // 开启ck
        env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE);
        // 设置检查点在被丢弃之前可能需要的最长时间
        env.getCheckpointConfig().setCheckpointTimeout(6000L);

        // 修改用户名，如果放在集群上跑，不需要设置
        System.setProperty("HADOOP_USER_NAME","atguigu");*/

        // 2. 读取kafka ods_base_log 主题数据
        String topic = "ods_base_log";
        String groupId = "ods_dwd_base_log_app";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        // 3. 将每行数据转换为JsonObejct
        OutputTag<String> dirtyOutputTag = new OutputTag<String>("dirty") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirtyOutputTag, value);
                }
            }
        });

        // 打印测试
//        jsonObjDS.print();
        DataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyOutputTag);
        dirtyDS.print("dirty>>>>>>");

        // 4. 按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(data -> data.getJSONObject("common").getString("mid"));

        // 5. 使用状态，做新老用户校验
        SingleOutputStreamOperator<JSONObject> jsonWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            // 声明状态用于表示当前mid是否已经访问过
            private ValueState<String> firstVisitDateState;
            private SimpleDateFormat simpleDateFormat;

            @Override
            public void open(Configuration parameters) throws Exception {
                firstVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("new-mid", String.class));
                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                // 取出新用户标记
                String isNew = value.getJSONObject("common").getString("is_new");
                // 如果当前前端传输数据表示为新用户，则进行校验
                if ("1".equals(isNew)) {
                    // 取出状态数据并取出当前访问时间
                    String firstDate = firstVisitDateState.value();
                    Long ts = value.getLong("ts");
                    // 判断状态数据是否为null
                    if (firstDate != null) {
                        // 修复
                        value.getJSONObject("common").put("is_new", "0");
                    } else {
                        // 更新状态
                        firstVisitDateState.update(simpleDateFormat.format(ts));
                    }
                }
                // 返回数据
                return value;
            }
        });

        // 打印测试
//        jsonWithNewFlagDS.print();


        // 6. 分流，使用ProcessFunction将ods数据拆分成自动、曝光以及页面数据
        SingleOutputStreamOperator<String> pageDS = jsonWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                // 提取"start"字段
                String startStr = jsonObject.getString("start");
                // 判断是否为启动数据
                if (startStr != null && startStr.length() > 0) {
                    // 将启动日志输出到侧输出流
                    ctx.output(new OutputTag<String>("start") {
                    }, jsonObject.toString());
                } else {
                    // 为页面数据，将数据输出到主流
                    out.collect(jsonObject.toJSONString());

                    // 不是启动数据，则继续判断是否是曝光数据
                    JSONArray displays = jsonObject.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        // 为曝光数据，便利写入侧输出流
                        for (int i = 0; i < displays.size(); i++) {
                            // 取出单挑曝光数据
                            JSONObject displayJson = displays.getJSONObject(i);
                            // 添加页面id
                            displayJson.put("page_id", jsonObject.getJSONObject("page").getString("page_id"));
                            // 输出到侧输出流
                            ctx.output(new OutputTag<String>("display") {
                            }, displayJson.toString());
                        }
                    }
                }
            }
        });

        // 7. 将三个流的数据写入到对应的Kafka主题
        DataStream<String> startDS = pageDS.getSideOutput(new OutputTag<String>("start") {
        });
        DataStream<String> displayDS = pageDS.getSideOutput(new OutputTag<String>("display"){});

        // 打印测试
        pageDS.print("page>>>>>>");
        startDS.print("start>>>>>>");
        displayDS.print("display>>>>>>");

        pageDS.addSink(MyKafkaUtil.getKafkaSink("dwd_page_log"));
        startDS.addSink(MyKafkaUtil.getKafkaSink("dwd_start_log"));
        displayDS.addSink(MyKafkaUtil.getKafkaSink("dwd_display_log"));


        // 执行任务
        env.execute();

    }
}
