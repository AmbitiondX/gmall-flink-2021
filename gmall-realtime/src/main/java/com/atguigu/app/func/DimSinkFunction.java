package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DimUtil;
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
    // 定义Phoenix连接
    private Connection connection;

    private PreparedStatement prepareStatement;


    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //jsonObject:{"id":"1001","name":"zhangsan","sinkTable":"dim_xxx_xxx"}
    //jsonObject:{"sinkTable":"dim_base_trademark","database":"gmall-flink-201109","data":{"tm_name":"sh","id":13},"type":"insert","before-data":{},"table":"base_trademark"}
    @Override
    public void invoke(JSONObject jsonObject, Context context) throws Exception {
        PreparedStatement preparedStatement = null;

        try {
            // 获取数据中的Key以及Value
            JSONObject data = jsonObject.getJSONObject("data");
            Set<String> keys = data.keySet();
            Collection<Object> values = data.values();

            // 获取表名
            String tableName = jsonObject.getString("sink_table");

            // 创建插入数据的SQL
            String upsertSql = genUpsertSql(tableName, keys, values);
            System.out.println(upsertSql);

            // 编译SQL
            prepareStatement = connection.prepareStatement(upsertSql);

            // 执行
            preparedStatement.executeUpdate();

            // 提交
            connection.commit();

            // 判断如果为更新数据，则删除Redis中数据
            if ("update".equals(jsonObject.getString("type"))) {
                String sourceTable = jsonObject.getString("table");
                String value = jsonObject.getJSONObject("data").getString("id");
                String key = sourceTable + ":" + value;
                DimUtil.deleteCached(key);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("插入Phoenix数据失败");
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
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

    private String genUpsertSql(String sinkTable, Set<String> keySet, Collection<Object> values) {
        return "upsert into" +
                GmallConfig.HBASE_SCHEMA + "." + sinkTable +
                "(" + StringUtils.join(keySet,",") + ")" +
                " values('" + StringUtils.join(values,"',") + "')";
    }

}
