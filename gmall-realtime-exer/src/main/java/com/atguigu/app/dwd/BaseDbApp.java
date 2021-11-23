package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.func.DimSinkFunction;
import com.atguigu.app.func.TableProcessFunction;
import com.atguigu.bean.TableProcess;
import com.atguigu.app.func.MyDeserializerFunc;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class BaseDbApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);   //生产环境应该与Kafka的分区数保持一致

        //开启CK 以及 指定状态后端
        //        env.enableCheckpointing(5 * 60000L);
        //        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //        env.setRestartStrategy();
        //
        //        env.setStateBackend(new FsStateBackend(""));

        //TODO 2.读取 Kafka ods_base_db 主题的数据创建流
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer("ods_base_db", "base_db_app"));

        //TODO 3.将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        //TODO 4.过滤空数据（删除数据）      主流
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return !"delete".equals(value.getString("type"));
            }
        });

        //TODO 5.使用FlinkCDC读取配置表创建  广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource
                .<String>builder()
                .hostname("hadoop102")
                .password("000000")
                .username("root")
                .databaseList("gmall-2021-realtime")
                .tableList("gmall-2121-realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyDeserializerFunc())
                .build();

        DataStreamSource<String> flinkCDCDS = env.addSource(sourceFunction);
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = flinkCDCDS.broadcast(mapStateDescriptor);

        //TODO 6.连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = filterDS.connect(broadcastStream);

        //TODO 7.处理广播流数据和主流数据   分为Kafka和HBase流
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbaseTag"){};
        SingleOutputStreamOperator<JSONObject> kafkaMainDS = connectedStream.process(new TableProcessFunction(hbaseTag, mapStateDescriptor));

        //TODO 8.提取两个流的数据
        DataStream<JSONObject> hbaseDS = kafkaMainDS.getSideOutput(hbaseTag);

        //TODO 9.将两个流的数据分别写出
        kafkaMainDS.print("Kafka>>>>>>>>");
        hbaseDS.print("HBase>>>>>>");

        hbaseDS.addSink(new DimSinkFunction());
        kafkaMainDS.addSink(MyKafkaUtil.getKafkaSink(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                return new ProducerRecord<>(jsonObject.getString("sinkTable"),
                        jsonObject.getString("data").getBytes());
            }
        }));


        //TODO 10.启动任务
        env.execute("BaseDbApp");

    }
}