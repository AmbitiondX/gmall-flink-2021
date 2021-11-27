package com.atguigu.app.func;

import com.alibaba.fastjson.JSON;
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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject,String,JSONObject> {

    private Connection connection;
    private PreparedStatement preparedStatement;

    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private OutputTag<JSONObject> hbaseTag;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor,OutputTag<JSONObject> hbaseTag) {
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

        // 将数据封装成javabean
        String data = JSON.parseObject(value).getString("data");
        TableProcess tableProcess = JSON.parseObject(data, TableProcess.class);

        // 判断是否需要创建表

//        System.out.println(TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType()));

        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {

            createTable(
                    tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend()
            );
        }

        // 设计广播状态的key,并广播状态
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        System.out.println("processBroadcastElement" + key);
        broadcastState.put(key,tableProcess);



    }

    // create table if not exists database.tablename (id varchar primary key varchar,name varchar,age varchar...) extend
    private void createTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        try {

            if (sinkPk == null ) {
                sinkPk = "id";
            }

            if (sinkExtend == null) {
                sinkExtend = "";
            }

            StringBuilder createSql = new StringBuilder("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");

            String[] columns = sinkColumns.split(",");

            for (int i = 0; i < columns.length; i++) {

                // 判断该字段是否为主键
                if (columns[i].equals(sinkPk)) {
                    createSql.append(columns[i]).append(" varchar").append(" primary key");
                } else {
                    createSql.append(columns[i]).append(" varchar");
                }

                // 判断该字段是否为最后一个字段
                if (i < columns.length - 1) {
                    createSql.append(",");
                }
            }

            createSql.append(") ").append(sinkExtend);
            System.out.println("建表语句：" + createSql);
            preparedStatement = connection.prepareStatement(createSql.toString());
            preparedStatement.execute();

        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("建表失败" + sinkTable);
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        // 提取广播状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        // 判断是否有此表的配置表信息
        String key = value.getString("table") + "-" + value.getString("type");
        System.out.println("processElement  " + key);
        TableProcess tableProcess = broadcastState.get(key);

        if (tableProcess != null) {
            // 过滤多余的字段
            filterColumn(value.getJSONObject("data"),tableProcess);
            System.out.println("toJSONString>>>>" + value.toJSONString());
            // 补充输出表字段
            value.put("sinkTable", tableProcess.getSinkTable());
            if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                ctx.output(hbaseTag,value);
            } else if (TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())) {
                out.collect(value);
            } else {
                System.out.println("程序有误！！！");
            }
        } else {
            System.out.println(key + "不存在");
        }

    }

    private void filterColumn(JSONObject value, TableProcess tableProcess) {
        String columns = tableProcess.getSinkColumns();
        String[] columnsArr = columns.split(",");
        List<String> list = Arrays.asList(columnsArr);

        Set<String> keySet = value.keySet();
        Iterator<String> iterator = keySet.iterator();
        while (iterator.hasNext()) {
            String next = iterator.next();
            if (!list.contains(next)){
                iterator.remove();
            }
        }
    }
}
