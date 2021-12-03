package com.atguigu.app.dws;

import com.atguigu.bean.ProvinceStats;
import com.atguigu.utils.ClickhouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {

        // 获取流和表的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 从kafka连接器中创建表格
        tableEnv.executeSql("CREATE TABLE order_wide (\n" +
                "  `province_id` BIGINT,\n" +
                "  `province_name` STRING,\n" +
                "  `province_area_code` STRING,\n" +
                "  `province_iso_code` STRING,\n" +
                "  `province_3166_2_code` STRING,\n" +
                "  `order_id` BIGINT,\n" +
                "  `split_total_amount` DECIMAL,\n" +
                "  `create_time` STRING,\n" +
                "  `rt` as to_timestamp(create_time),\n" +
                "   WATERMARK FOR rt AS rt - INTERVAL '1' SECOND\n" +
                ") WITH " + MyKafkaUtil.getKafkaDDL("dwm_order_wide", "ProvinceStatsSqlApp"));


        // 查询表格
        Table sqlQuery = tableEnv.sqlQuery(
                "select\n" +
                "    DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt,\n" +
                "    DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt,\n" +
                "    province_id,\n" +
                "    province_name,\n" +
                "    province_area_code,\n" +
                "    province_iso_code,\n" +
                "    province_3166_2_code,\n" +
                "    count(distinct order_id) order_count,\n" +
                "    sum(split_total_amount) order_amount,\n" +
                "    UNIX_TIMESTAMP()*1000 ts\n" +
                "from order_wide\n" +
                "group by \n" +
                "    province_id,\n" +
                "    province_name,\n" +
                "    province_area_code,\n" +
                "    province_iso_code,\n" +
                "    province_3166_2_code,\n" +
                "    TUMBLE(rt, INTERVAL '10' SECOND)");

        // 将表转为流
        DataStream<ProvinceStats> provinceStatsDS = tableEnv.toAppendStream(sqlQuery, ProvinceStats.class);

        // 把流写入到clickhouse
        provinceStatsDS.print(">>>>>>>>>>>>");
        provinceStatsDS.addSink(ClickhouseUtil.getJdbcSink("insert into province_stats_2021 values(?,?,?,?,?,?,?,?,?,?)"));


        env.execute("ProvinceStatsSqlApp");


    }
}
