package com.atguigu.utils;

import com.atguigu.bean.TransientSink;
import com.atguigu.common.GmallConfig;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickhouseUtil {

    public static <T> SinkFunction<T> getJdbcSink (String sql) {

        return JdbcSink.sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @SneakyThrows
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {

                        Class<?> clz = t.getClass();

                        Field[] declaredFields = clz.getDeclaredFields();

                        int offset = 0;
                        for (int i = 0; i < declaredFields.length; i++) {

                            Field declaredField = declaredFields[i];
                            declaredField.setAccessible(true);

                            TransientSink annotation = declaredField.getAnnotation(TransientSink.class);

                            if (annotation != null) {
                                offset++;
                                continue;
                            }

                            Object value = declaredField.get(t);

                            preparedStatement.setObject(i + 1 - offset,value);

                        }


                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .withBatchIntervalMs(30000).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL).build()
        );


    }

}
