package com.atguigu.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.func.MyDeserializerFunc;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink_CDC {
    public static void main(String[] args) throws Exception {
        // 获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        // 创建Flink-mySQL-CDC 的SOURCE
        DebeziumSourceFunction<String> sourceFunction = MySQLSource
                .<String>builder()
                .hostname("hadoop102")
                .username("root")
                .password("root")
                .databaseList("gmall_rt")
                .startupOptions(StartupOptions.latest())
                .deserializer(new MyDeserializerFunc())
                .build();

        // 使用CDC Source 从Mysql读取数据
        DataStreamSource<String> mysqlDS = env.addSource(sourceFunction);

        // 测试：打印流
        mysqlDS.print();

        // 将流中的数据写入kafka
        mysqlDS.addSink(MyKafkaUtil.getFlinkKafkaProducer("ods_base_db"));


        env.execute();
    }
}
