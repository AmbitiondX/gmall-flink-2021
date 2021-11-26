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
    PreparedStatement preparedStatement;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        preparedStatement = null;

        try {
            String upsertSql = genUpsertSQL(value);
            System.out.println(upsertSql);

            // 预编译SQL
            preparedStatement = connection.prepareStatement(upsertSql);

            // 执行SQL
            preparedStatement.execute();

            // 手动提交(默认是批量提交，为了更快的显示结果，改为手动提交)
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("插入数据失败！");
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }

    }


    // upsert into database.tableName (id,name,other...) values ('id','name','other'...)
    private String genUpsertSQL(JSONObject jsonObject) {

        JSONObject data = jsonObject.getJSONObject("data");

        // 获取字段名
        Set<String> keySet = data.keySet();
        String keys = StringUtils.join(keySet, ",");

        // 获取字段值
        Collection<Object> valueColl = data.values();
        String values = StringUtils.join(valueColl, "','");

        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + jsonObject.getString("sinkTable") + "(" + keys + ") values('" + values + "')";
    }
}
