package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collection;
import java.util.Set;


public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    // 定义Phoenix连接
    private Connection connection;


    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //jsonObject:{"id":"1001","name":"zhangsan","sinkTable":"dim_xxx_xxx"}
    //jsonObject:{"sinkTable":"dim_base_trademark","database":"gmall-flink-201109","data":{"tm_name":"sh","id":13},"type":"insert","before-data":{},"table":"base_trademark"}
    @Override
    public void invoke(JSONObject jsonObject, Context context) throws Exception {
        //准备写入数据的SQL  upsert into xx.xx(id,name) values('1001','zhangsan')
        JSONObject data = jsonObject.getJSONObject("data");
        String sinkTable = jsonObject.getString("sinkTable");
        genUpsertSql(data,sinkTable);
    }

    //upsert into xx.xx(id,name) values('1001','zhangsan','male')
    private String genUpsertSql(JSONObject data, String sinkTable) {
        //取出数据中的Key和Value集合
        Set<String> keySet = data.keySet();
        Collection<Object> values = data.values();

        return "upsert into" +
                GmallConfig.HBASE_SCHEMA + "." + sinkTable +
                "(" + StringUtils.join(keySet,",") + ")" +
                " values('" + StringUtils.join(values,"',") + "')";
    }
}
