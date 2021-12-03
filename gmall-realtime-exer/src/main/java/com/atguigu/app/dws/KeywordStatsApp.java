package com.atguigu.app.dws;

import com.atguigu.app.udf.KeywordUDTF;

import com.atguigu.bean.KeywordStats;
import com.atguigu.utils.ClickhouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {

        //TODO 0.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);
        /*
        //CK相关设置
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend(
                "hdfs://hadoop102:8020/gmall/flink/checkpoint/ProvinceStatsSqlApp");
        env.setStateBackend(fsStateBackend);
        System.setProperty("HADOOP_USER_NAME","atguigu");
        */
        //TODO 1.定义Table流环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //TODO 2.注册自定义函数
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);

        //TODO 3.将数据源定义为动态表
        String groupId = "keyword_stats_app";
        String pageViewSourceTopic ="dwd_page_log";

        tableEnv.executeSql(
                "CREATE TABLE page_view (\n" +
                        "  `page` Map<String,String>,\n" +
                        "  `ts` BIGINT,\n" +
                        "  `rt` as to_timestamp(from_unixtime(ts/1000)),\n" +
                        "   WATERMARK FOR rt AS rt - INTERVAL '1' SECOND\n" +
                        ") WITH " + MyKafkaUtil.getKafkaDDL(pageViewSourceTopic,groupId)
        );

        //TODO 4.过滤数据,只需要搜索的日志
        Table filterTable = tableEnv.sqlQuery(
                "select\n" +
                        "    page['item'] key_word,\n" +
                        "    rt\n" +
                        "from page_view\n" +
                        "where page['item_type'] = 'keyword'\n" +
                        "and page['item'] is not null"
        );

        //TODO 5.使用UDTF进行分词
        tableEnv.createTemporaryView("filter_table", filterTable);
        Table word_table = tableEnv.sqlQuery(
                "select\n" +
                        "    word,\n" +
                        "    rt\n" +
                        "from \n" +
                        "    filter_table, lateral table(ik_analyze(key_word))"
        );
        tableEnv.createTemporaryView("word_table", word_table);



        //TODO 6.分组开窗、聚合

        Table resultTable = tableEnv.sqlQuery(
                "select\n" +
                        "    'search' source,\n" +
                        "    DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt,\n" +
                        "    DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt,\n" +
                        "    word keyword,\n" +
                        "    count(*) ct,\n" +
                        "    UNIX_TIMESTAMP()*1000 ts\n" +
                        "from word_table\n" +
                        "group by\n" +
                        "    TUMBLE(rt, INTERVAL '10' SECOND),\n" +
                        "    word"
        );

        //TODO 7.将动态表转换为流
        DataStream<KeywordStats> keywordStatsDS = tableEnv.toAppendStream(resultTable, KeywordStats.class);

        //TODO 8.将数据写出到ClickHouse
        keywordStatsDS.addSink(ClickhouseUtil.getJdbcSink("insert into keyword_stats_2021 (keyword,ct,source,stt,edt,ts) values (?,?,?,?,?,?)"));

        env.execute();


    }
}
