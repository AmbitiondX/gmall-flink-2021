package com.atguigu.app.ods;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;

import com.atguigu.utils.MyKafkaUtil;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;

import org.apache.kafka.connect.data.Struct;

import java.util.Locale;

public class Flink_CDCWithCustomerSchema {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        // 创建Flink-mySQL-CDC 的SOURCE
        DebeziumSourceFunction<String> mysqlSource = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("root")
                .databaseList("gmall_rt")
                .startupOptions(StartupOptions.latest())
                .deserializer(new DebeziumDeserializationSchema<String>() {
                    @Override
                    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
                        // 获取主题信息，包含这数据库和表明
                        String topic = sourceRecord.topic();


                        String[] arr = topic.split("\\.");
                        String db = arr[1];
                        String tableName = arr[2];

                        // 获取操作符类型 READ DELETE UPDATE CREATE
                        Envelope.Operation operation = Envelope.operationFor(sourceRecord);

                        // 获取信息并转化为Struct类型
                        Struct value = (Struct) sourceRecord.value();

                        // 获取变化后的数据
                        Struct after = value.getStruct("after");
                        // 创建json对象用于存储数据信息
                        JSONObject jsonObject = new JSONObject();
                        if (after != null) {
                            Schema schema = after.schema();
                            for (Field field : schema.fields()) {
                                jsonObject.put(field.name(), after.get(field.name()));
                            }
                        }

                        //获取Value信息,提取删除或者修改的数据本身
                        Struct before = value.getStruct("before");
                        JSONObject beforeJson = new JSONObject();
                        if (before != null) {
                            for (Field field : before.schema().fields()) {
                                Object o = before.get(field);
                                beforeJson.put(field.name(), o);
                            }
                        }

                        // 创建json对象用于封装最终返回值数据信息
                        JSONObject result = new JSONObject();
                        result.put("database", db);
                        result.put("table", tableName);
                        result.put("data", jsonObject );
                        result.put("before-data", beforeJson);
                        String type = operation.toString().toLowerCase();
                        if ("create".equals(type)) {
                            type = "insert";
                        }
                        result.put("type", type);

                        // 发送数据至下游
                        collector.collect(result.toJSONString());
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return  BasicTypeInfo.STRING_TYPE_INFO;
                    }
                })
                .build();

        // 3.使用CDC Source 从Mysql读取数据
        DataStreamSource<String> mysqlDS = env.addSource(mysqlSource);

        // 4.打印数据
        mysqlDS.addSink(MyKafkaUtil.getKafkaSink("ods_base_db"));

        // 5.执行任务
        env.execute();
    }
}
