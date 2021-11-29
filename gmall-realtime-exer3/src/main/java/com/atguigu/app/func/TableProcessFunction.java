package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject,String,JSONObject> {

    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private OutputTag<JSONObject> hbaseTag;

    private Connection connection;
    private PreparedStatement preparedStatement;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor, OutputTag<JSONObject> hbaseTag) {
        this.mapStateDescriptor = mapStateDescriptor;
        this.hbaseTag = hbaseTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        // 将数据转化为javaBean
        JSONObject jsonObject = JSONObject.parseObject(value);
        TableProcess tableProcess = JSONObject.parseObject(jsonObject.getString("data"), TableProcess.class);

        // 判断tableProcess类型，是否需要建表
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            checkTable(
                    tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend()
            );
        }

        // 广播变量
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        broadcastState.put(key,tableProcess);

    }

    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) throws SQLException {
        // create table database.tablename (id varchar primary,name varchar,age varchar...)
        try {

            if (sinkPk == null) {
                sinkPk = "id";
            }

            if (sinkExtend == null) {
                sinkExtend = "";
            }

            StringBuilder createSQL = new StringBuilder("create table ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append(" (");


            String[] columns = sinkColumns.split(",");

            for (int i = 0; i < columns.length; i++) {

                if (sinkPk.equals(columns[i])) {
                    createSQL.append(columns[i]).append(" varchar").append(" parmary key");
                } else {
                    createSQL.append(columns[i]).append(" varchar");
                }

                if (i < columns.length - 1) {
                    createSQL.append(",");
                }
            }

            createSQL.append(")").append(sinkExtend);

            System.out.println("建表语句=" + createSQL);

            preparedStatement = connection.prepareStatement(createSQL.toString());
            preparedStatement.execute();
        } catch (SQLException e) {
            System.out.println("建表失败" + sinkTable);
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }

    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        // 获取广播状态
        String key = value.getString("table") + ":" + value.getString("type");
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        TableProcess tableProcess = broadcastState.get(key);

        if (tableProcess != null) {

            // 过滤字段
            filterColumns(value.getJSONObject("data"),tableProcess.getSinkColumns());

            // 设置输出表名
            value.put("sinkTable",tableProcess.getSinkTable());


            if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                ctx.output(hbaseTag, value);
            } else if (TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())) {
                out.collect(value);
            }

        } else {
            System.out.println("key不存在=" + key);
        }
    }

    private void filterColumns(JSONObject data, String sinkColumns) {

        String[] columns = sinkColumns.split(",");
        List<String> list = Arrays.asList(columns);

        Set<Map.Entry<String, Object>> entries = data.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> next = iterator.next();
            if (list.contains(next.getKey())) {
                iterator.remove();
            }
        }

    }
}
