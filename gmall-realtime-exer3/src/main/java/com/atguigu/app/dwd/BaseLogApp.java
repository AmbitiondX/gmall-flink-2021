package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //并行度设置为Kafka的分区数

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

        //TODO 2.读取Kafka ods_base_log 主题的数据
        String sourceTopic = "ods_base_log";
        String groupId = "base_log_app_group";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(sourceTopic, groupId));

        //TODO 3.将每行数据转换为JSONObject
        OutputTag<String> dirty = new OutputTag<String>("DirtyData") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirty, value);
                }
            }
        });

        //TODO 4.按照设备ID分组、使用状态编程做新老用户校验
        SingleOutputStreamOperator<JSONObject> jsonWithNewFlagDS = jsonObjDS
                .keyBy(json -> json.getJSONObject("common").getString("mid"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {

                    // 定义状态
                    private ValueState<String> firstVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 初始化状态
                        firstVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("is_new", String.class));
                    }

                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        // 从对象中取出状态
                        String uncertainState = value.getJSONObject("common").getString("is_new");

                        String state = firstVisitDateState.value();
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        String date = sdf.format(System.currentTimeMillis());

                        if ("1".equals(uncertainState)) {
                            // 检验是否真正为新用户
                            if (state == null || date.equals(state)) {
                                // 此时确实为新用户，将状态更新为1
                                firstVisitDateState.update(date);
                            } else {
                                // 说明当前mid不是新用户
                                value.getJSONObject("common").put("is_new", "0");
                            }
                        }

                        return value;
                    }
                });

        //TODO 6.分流  将页面日志  主流   启动和曝光  侧输出流
        OutputTag<JSONObject> startTag = new OutputTag<JSONObject>("start") {
        };
        OutputTag<JSONObject> displayTag = new OutputTag<JSONObject>("display") {
        };

        SingleOutputStreamOperator<JSONObject> pageDS = jsonWithNewFlagDS.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                // 获取启动数据
                JSONObject start = value.getJSONObject("start");
                if (start != null) {
                    // 将启动数据谢日启动日志侧输出流
                    ctx.output(startTag, value);
                } else {

                    // 将数据写入页面日志输出流
                    out.collect(value);

                    // 获取曝光数据字段
                    JSONArray displays = value.getJSONArray("display");

                    // 判断曝光数据是否存在
                    Long ts = value.getLong("ts");
                    String pageId = value.getJSONObject("page").getString("page_id");
                    if (displays != null && displays.size() > 0) {

                        // 遍历曝光数据，写出数据到曝光侧输出流
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject jsonObject = displays.getJSONObject(i);

                            // 补充页面id和时间戳字段
                            jsonObject.put("ts", ts);
                            jsonObject.put("page_id", pageId);

                            // 将曝光数据写出到曝光侧输出流
                            ctx.output(displayTag, jsonObject);
                        }
                    }
                }
            }
        });

        // TODO 7.提取各个流的数据
        DataStream<JSONObject> startDS = pageDS.getSideOutput(startTag);
        DataStream<JSONObject> displayDS = pageDS.getSideOutput(displayTag);

        // 打印测试
        pageDS.print("Page>>>>>>>>>>>");
        startDS.print("Start>>>>>>>>>>>");
        displayDS.print("Display>>>>>>>>>");

        //TODO 8.将数据写入Kafka主题
        pageDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getFlinkKafkaProducer("dwd_page_log"));
        startDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getFlinkKafkaProducer("dwd_start_log"));
        displayDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getFlinkKafkaProducer("dwd_display_log"));

        //TODO 9.启动任务
        env.execute();


    }
}
