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

        // upsert table database.tablename (id,name,age) values ('id','name','age')

        try {
            Set<String> keySet = value.keySet();
            String keys = StringUtils.join(keySet, ",");

            Collection<Object> valueColl = value.values();
            String values = StringUtils.join(valueColl, "','");

            StringBuilder upsertSql = new StringBuilder("upsert table ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(value.getString("sinkTable"))
                    .append(" (")
                    .append(keys)
                    .append(") values ('")
                    .append(values)
                    .append("')");

            preparedStatement = connection.prepareStatement(upsertSql.toString());
            preparedStatement.execute();
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("插入数据失败");
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }
}
