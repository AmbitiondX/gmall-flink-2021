package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DimUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    //声明Phoenix连接
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

//        connection.setAutoCommit(true);
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement = null;

        try {
            //拼接插入数据的SQL语句  upsert into db.tn(id,name,sex) values('1001','zhangsan','male')
            String upsertSQL = genUpsertSQL(value.getString("sinkTable"), value.getJSONObject("data"));

            //预编译SQL
            preparedStatement = connection.prepareStatement(upsertSQL);

            // 如果维度数据更新，则先删除redis中的数据 ***************
            if ("update".equals(value.getString("type"))){
                DimUtil.delDimInfo(value.getString("sinkTable").toLowerCase(), value.getJSONObject("data").getString("id"));
            }

            // 执行写入数据操作
            preparedStatement.execute();

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

    private String genUpsertSQL(String sinkTable, JSONObject data) {
        // 获取列名和数据
        Set<String> columns = data.keySet();
        Collection<Object> values = data.values();

        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" +
                StringUtils.join(columns,",") + ") values ('" +
                StringUtils.join(values,"','") + "')";
    }

    // 测试StringUtils.join
    public static void main(String[] args) {
        ArrayList<String> list = new ArrayList<>();
        list.add("id");
        list.add("name");
        list.add("score");
        System.out.println(list);
        /*
            [id, name, score]
            upsert into GMALL2021_REALTIME.sinkTable(id,name,score) values ('id','name','score')
         */
        System.out.println("upsert into " + GmallConfig.HBASE_SCHEMA + "." + "sinkTable" + "(" +
                StringUtils.join(list,",") + ") values ('" +
                StringUtils.join(list,"','") + "')");
    }

}
