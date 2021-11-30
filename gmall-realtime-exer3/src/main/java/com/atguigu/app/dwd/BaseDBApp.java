package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
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
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /*env.setParallelism(1);
        // 设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/dwd_log/ck"));
        // 开启ck
        env.enableCheckpointing(7000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(6000L);

        // 修改用户名，如果放在集群上跑，不需要设置
        System.setProperty("HADOOP_USER_NAME","atguigu");*/

        String sourceTable = "ods_base_db";
        String groupId = "ods_base_db_consumer";

        // 2.获取流
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(sourceTable, groupId));

        // 3.将数据json对象,过滤脏数据
        OutputTag<String> ditrtyTag = new OutputTag<String>("ditrtyTag"){};
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(ditrtyTag, value);
                }
            }
        });

        //4. 过滤type=delete的数据
        SingleOutputStreamOperator<JSONObject> filterDS = jsonDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return !"delete".equals(value.getString("type"));
            }
        });

        // 获取配置表
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

        DataStreamSource<String> tableProcessDS = env.addSource(sourceFunction);

        // 将tableProcessDS转化为广播流
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<String, TableProcess>("mapState",String.class,TableProcess.class);
        BroadcastStream<String> broadcast = tableProcessDS.broadcast(mapStateDescriptor);


        // 连接filterDS和广播流连接
        BroadcastConnectedStream<JSONObject, String> connect = filterDS.connect(broadcast);

        // 处理BroadcastConnectedStream
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase") {
        };
        SingleOutputStreamOperator<JSONObject> kafkaMainDS = connect.process(new TableProcessFunction(mapStateDescriptor, hbaseTag));

        // 获取侧输出流
        DataStream<JSONObject> hbaseDS = kafkaMainDS.getSideOutput(hbaseTag);

        hbaseDS.print("hbaseDS>>>>>");
        kafkaMainDS.print("kafka>>>>>");

        // 写出
        hbaseDS.addSink(new DimSinkFunction());

        kafkaMainDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(new KafkaSerializationSchema<JSONObject>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                        return new ProducerRecord<>(element.getString("sinkTable"), element.getString("data").getBytes());
                    }
                }));


        env.execute();
    }
}
