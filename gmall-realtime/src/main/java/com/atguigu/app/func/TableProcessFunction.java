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
import java.util.*;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    // 定义侧输出流标签
    private OutputTag<JSONObject> outputTag;

    // 定义Map状态表述器
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction(OutputTag<JSONObject> outputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.outputTag = outputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    // 定义Phoenix连接
    private Connection connection = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化Phoenix的连接
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        // 获取状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);

        // 获取表明和操作类型
        String table = jsonObject.getString("table");
        String type = jsonObject.getString("type");
        String key = table + ":" + type;

        // 取出对应的配置信息数据
        TableProcess tableProcess = broadcastState.get(key);

        if (tableProcess != null) {
            // 根据配置信息过滤字段
            JSONObject data = jsonObject.getJSONObject("data");
            String sinkColumns = tableProcess.getSinkColumns();
            filterColumn(data,sinkColumns);

            // 分流，将Hbase数据接入侧输出流，kafka数据写入主流
            String sinkType = tableProcess.getSinkType();

            // 将待写入的维度表或者主题名添加至数据中，方便后续操作
            jsonObject.put("sinkTable", tableProcess.getSinkTable());

            if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)){
                readOnlyContext.output(outputTag, jsonObject);
            } else if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)){
                collector.collect(jsonObject);
            }
        } else {
            System.out.println("配置信息中不存在key：" + key);
        }
    }

    /**
     * 根据配置信息过滤字段
     *
     * @param data        待过滤的数据
     * @param sinkColumns 目标字段
     */
    private void filterColumn(JSONObject data, String sinkColumns) {
        // 处理目标字段
        String[] columns = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(columns);

        // 遍历data
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        while (iterator.hasNext()){
            Map.Entry<String, Object> next = iterator.next();
            if (!columnList.contains(next.getKey())) {
                iterator.remove();
            }
        }
    }

    @Override
    public void processBroadcastElement(String jsonStr, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        // 获取状态
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);

        // 将配置信息流中的数据转换为JSON对象，{"database":"","table":"","type","","data":{"":""}}
        JSONObject jsonObject = JSON.parseObject(jsonStr);

        // 取出数据中的表明以及操作类型封装key
        JSONObject data = jsonObject.getJSONObject("data");
        TableProcess tableProcess = JSON.parseObject(data.toJSONString(), TableProcess.class);

        if (tableProcess != null) {
            // 校验表是否存在秒如果不存在，则创建Phoenix表
            if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())){
                // 建表
                checkTable(
                        tableProcess.getSinkTable(),
                        tableProcess.getSinkColumns(),
                        tableProcess.getSinkPk(),
                        tableProcess.getSinkExtend());
            }
            //将数据写入状态广播处理
            String key = tableProcess.getSourceTable() + ":" + tableProcess.getOperateType();
            broadcastState.put(key, tableProcess);
        }
    }


    /**
     * Phoenix建表
     *
     * @param sinkTable   表名       test
     * @param sinkColumns 表名字段   id,name,sex
     * @param sinkPk      表主键     id
     * @param sinkExtend  表扩展字段 ""
     *                    create table if not exists mydb.test(id varchar primary key,name varchar,sex varchar) ...
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        // 给主键以及扩展字段默认值
        if (sinkPk == null) {
            sinkPk = "id";
        }

        if (sinkExtend == null) {
            sinkExtend = "";
        }

        // 封装SQL
        StringBuilder createSql = new StringBuilder("create table if not exists")
                .append(GmallConfig.HBASE_SCHEMA)
                .append(".")
                .append(sinkTable)
                .append("(");

        // 遍历添加字段信息
        String[] fields = sinkColumns.split(",");
        for (int i = 0; i < fields.length; i++) {
            // 取出字段
            String field = fields[i];

            // 判断当前字段是否为主键
            if (sinkPk.equals(field)){
                createSql.append(field).append(" varchar primary key");
            } else {
                createSql.append(field).append(" varchar");
            }

            // 如果当前字段不是最后一个字段，则追加","
            if (i < fields.length - 1) {
                createSql.append(",");
            }
        }

        // 打印语句，观察是否有误
        System.out.println(createSql);

        // 执行建表SQL
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(createSql.toString());
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("创建Phoenix表" + sinkTable + "失败!");
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
}
