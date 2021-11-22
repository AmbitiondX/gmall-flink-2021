package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class MyDeserializerFunc implements DebeziumDeserializationSchema<String> {
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
}
