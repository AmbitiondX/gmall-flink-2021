package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.func.DimSinkFunction;
import com.atguigu.app.func.MyDeserializerFunc;
import com.atguigu.app.func.TableProcessFunction;
import com.atguigu.bean.TableProcess;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/dwd_log/ck"));
        // 开启ck
        env.enableCheckpointing(7000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(6000L);

        // 修改用户名，如果放在集群上跑，不需要设置
        System.setProperty("HADOOP_USER_NAME","atguigu");

        //TODO 2.获取ods_base_db数据流
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaSource("ods_base_db", "ods_base_db_consumer"));

        //TODO 3.将每行数据转换为JSON对象 过滤脏数据
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println("dirty>>>>>>" + value);
                }
            }
        });

        //TODO 4.过滤删除(type=delete)数据
        //生产环境中一般不会生成delete的数据，但是学习的脚本有delete的数据，所以过滤为好
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return !"delete".equals(value.getString("type"));
            }
        });

        //TODO 4.使用flinkCDC获取table_process的数据，并转为JavaBean，做成广播流，将流广播出去
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("root")
                .databaseList("gmall2021_realtime")
                .tableList("gmall2021_realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyDeserializerFunc())
                .build();
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("mapStateDescriptor", String.class, TableProcess.class);

        BroadcastStream<String> broadcastStream = streamSource.broadcast(mapStateDescriptor);

        //TODO 5.connect主流、广播流
        BroadcastConnectedStream<JSONObject, String> connect = filterDS.connect(broadcastStream);

        //TODO 6.使用process方法，处理两条流
        OutputTag<JSONObject> outputTagHbase = new OutputTag<JSONObject>("hbase") {
        };
        SingleOutputStreamOperator<JSONObject> kafkaMainDS = connect.process(new TableProcessFunction(mapStateDescriptor, outputTagHbase));

        //TODO 7.分流，并写出
        DataStream<JSONObject> hbaseDS = kafkaMainDS.getSideOutput(outputTagHbase);
        hbaseDS.print("hbase>>>>>>");
        kafkaMainDS.print("kafka>>>>>>");

        hbaseDS.addSink(new DimSinkFunction());



        //TODO 8.执行任务
        env.execute();
    }
}
