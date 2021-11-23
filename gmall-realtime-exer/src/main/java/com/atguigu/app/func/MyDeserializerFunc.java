package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;


/*
    原始数据：
    SourceRecord{sourcePartition={server=mysql_binlog_source}, sourceOffset={file=mysql-bin.000020, pos=18778057, row=1, snapshot=true}} ConnectRecord{topic='mysql_binlog_source.gmall_flink.z_user_info', kafkaPartition=null, key=null, keySchema=null, value=Struct{after=Struct{id=1,name=zs},source=Struct{version=1.4.1.Final,connector=mysql,name=mysql_binlog_source,ts_ms=0,snapshot=true,db=gmall_flink,table=z_user_info,server_id=0,file=mysql-bin.000020,pos=18778057,row=0},op=c,ts_ms=1637578337601}, valueSchema=Schema{mysql_binlog_source.gmall_flink.z_user_info.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}

    封装后的格式：
    {
        "database": "gmall_rt",
        "data": {
            "create_time": "2021-11-20 20:50:18",
            "user_id": 2607,
            "appraise": "1204",
            "comment_txt": "评论内容：21739783773322751947266522818317644882123834684432",
            "sku_id": 34,
            "id": 1462765382466170905,
            "spu_id": 12,
            "order_id": 28007
        },
        "type": "insert",
        "before-data": {},
        "table": "comment_info"
    }
 */
public class MyDeserializerFunc implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        // 创建jsonObject
        JSONObject result = new JSONObject();

        //TODO 1.获取库名和表明
        // topic='mysql_binlog_source.gmall_flink.z_user_info'
        String topic = sourceRecord.topic();
        String[] split = topic.split("\\.");
        String databasesName = split[1];
        String tableName = split[2];

        //TODO 2.获取after的数据
        Struct value = (Struct) sourceRecord.value();
        Struct afterStruct = value.getStruct("after");
        JSONObject afterJsonObj = new JSONObject();
        if (afterStruct != null) {
            List<Field> fieldList = afterStruct.schema().fields();
            for (Field field : fieldList) {
                afterJsonObj.put(field.name(), afterStruct.get(field));
            }
        }

        //TODO 3.获取before的数据
        Struct beforeStruct = value.getStruct("before");
        JSONObject beforeJsonObj = new JSONObject();
        if (beforeStruct != null) {
            List<Field> fieldList = beforeStruct.schema().fields();
            for (Field field : fieldList) {
                beforeJsonObj.put(field.name(), beforeStruct.get(field));
            }
        }

        //TODO 4.获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)){
            type = "insert";
        }

        //TODO 5.将数据封装到result
        result.put("data", afterJsonObj);
        result.put("before-data", beforeJsonObj);
        result.put("database", databasesName);
        result.put("table", tableName);
        result.put("type", type);

        //TODO 6.收集jsonObject
        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
