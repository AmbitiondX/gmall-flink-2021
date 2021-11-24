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

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    private Connection connection;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private OutputTag<JSONObject> outputTag;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor, OutputTag<JSONObject> outputTag){
        this.mapStateDescriptor = mapStateDescriptor;
        this.outputTag = outputTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 注册驱动，并获取连接
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    // value: {"table":"","database":"","data":{...},"before-data":{...},"type":""}
    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        // 1.解析为javaBean
        String data = JSONObject.parseObject(value).getString("data");
        TableProcess tableProcess = JSONObject.parseObject(data, TableProcess.class);

        // 2.判断表是否存在，如果不存在，则建表
        // 使用 create table if not exists 可以避免判断
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            checkTable(
                    tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend()
            );
        }

        // 3.写入状态，将状态广播出去
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        broadcastState.put(key, tableProcess);
    }

    // 建表语句示例：create table is not exists dim_base_trademark (id varchar primary key,tm_name varchar) extend
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        PreparedStatement preparedStatement = null;
        try {
        // 避免sinkExtend为null时，建表语句最后拼接 null
        if (sinkExtend == null) {
            sinkExtend = "";
        }

        // 默认id为主键
        if (sinkPk == null || sinkPk.length() == 0) {
            sinkPk = "id";
        }

        StringBuilder createTableSQL = new StringBuilder("create table if not exists ")
                .append(GmallConfig.HBASE_SCHEMA)
                .append(".")
                .append(sinkTable)
                .append("(");

        // 分解列
        String[] columns = sinkColumns.split(",");
        for (int i = 0; i < columns.length; i++) {

            //取出列名
            String column = columns[i];

            // 判断column是否为主键
            if (column.equals(sinkPk)) {
                createTableSQL.append(column).append(" varchar primary key ");
            } else {
                createTableSQL.append(column).append(" varchar");
            }

            //判断当前如果不是最后一个字段,则添加","
            if (i < columns.length - 1) {
                createTableSQL.append(", ");
            }
        }

        createTableSQL.append(")").append(sinkExtend);

        // 打印SQL
        System.out.println(createTableSQL);


            preparedStatement = connection.prepareStatement(createTableSQL.toString());
            preparedStatement.execute();
        } catch (SQLException e) {
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

    //value:{"db":"","tableName":"base_trademark","before":{},"after":{"id":"","tm_name":"","logo_url":""},"type":""}
    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //1.根据主键读取广播状态对应的数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = value.getString("table") + "-" + value.getString("type");

        TableProcess tableProcess = broadcastState.get(key);

        // ods层的表比dwd层的表要多，所以dwd层的数据无法完全涵盖ods层的表，因此需要做null值判断
        if (tableProcess != null) {
            //2.根据广播状态数据  过滤字段
            filterColumns(value.getJSONObject("data"),tableProcess.getSinkColumns());

            //3.根据广播状态数据  分流   主流：Kafka   侧输出流：HBase
            if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                ctx.output(outputTag,value);
            } else if (TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())) {
                out.collect(value);
            }
        } else {
            // key不存在 标准方式：打印到logger 不建议在控制台
            System.out.println(key + "不存在！");
        }


    }

    //after:{"id":"","tm_name":"","logo_url":"","name":""}  sinkColumns:id,tm_name
    //{"id":"","tm_name":""}
    private void filterColumns(JSONObject jsonObject, String sinkColumns) {
        String[] columns = sinkColumns.split(",");
        List<String> list = Arrays.asList(columns);

        Set<String> keySet = jsonObject.keySet();

//        Set<Map.Entry<String, Object>> entries = jsonObject.entrySet();
//        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
//        while (iterator.hasNext()) {
//            Map.Entry<String, Object> next = iterator.next();
//            if (!list.contains(next.getKey())) {
//                iterator.remove();
//            }
//        }

        Set<Map.Entry<String, Object>> entries = jsonObject.entrySet();
        entries.removeIf(next -> !list.contains(next.getKey()));
    }
}
