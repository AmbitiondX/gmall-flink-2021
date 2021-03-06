package com.atguigu.app.ods;


import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.func.MyDeserializerFunc;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class Flink_CDCWithCustomerSchema {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        // 创建Flink-mySQL-CDC 的SOURCE
        DebeziumSourceFunction<String> mysqlSource = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("root")
                .databaseList("gmall_rt")
                .startupOptions(StartupOptions.latest())
                .deserializer(new MyDeserializerFunc())
                .build();

        // 3.使用CDC Source 从Mysql读取数据
        DataStreamSource<String> mysqlDS = env.addSource(mysqlSource);

        // 4.打印数据
        mysqlDS.print("mysqlDS>>>>>");
        mysqlDS.addSink(MyKafkaUtil.getKafkaSink("ods_base_db"));

        // 5.执行任务
        env.execute();
    }
}
