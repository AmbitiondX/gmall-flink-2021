package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private Connection connection;
    private PreparedStatement preparedStatement;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {

        // upsert into database.tablename (id,name,age...) values ('id','name','age')

        try {
            JSONObject data = value.getJSONObject("data");
            Set<String> keys = data.keySet();
            Collection<Object> values = data.values();


            StringBuilder upsertSQL = new StringBuilder("upsert into ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(value.getString("table"))
                    .append("(")
                    .append(StringUtils.join(keys,","))
                    .append(") values ('")
                    .append(StringUtils.join(values,"','"))
                    .append("')");

            System.out.println("upsert语句=" + upsertSQL);

            preparedStatement = connection.prepareStatement(upsertSQL.toString());
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException("插入数据失败：" + value);
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }

    }
}
