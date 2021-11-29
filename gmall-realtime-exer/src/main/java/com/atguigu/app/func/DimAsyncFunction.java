package com.atguigu.app.func;


import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DimUtil;

import com.atguigu.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {

    private Connection connection;
    private String tableName;
    private ThreadPoolExecutor threadPoolExecutor;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        threadPoolExecutor = ThreadPoolUtil.getThreadPoolExecutor();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture)   {

        threadPoolExecutor.submit(new Runnable() {
            @Override
            public void run() {

                String key = getKey(input);

                try {

                    //读取维度信息
                    JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, key);

                    //将维度信息补充至数据
                    if (dimInfo != null) {
                        join(input,dimInfo);
                    }

                    //将补充完成的数据写出
                    resultFuture.complete(Collections.singleton(input));

                } catch (SQLException | InvocationTargetException | InstantiationException | IllegalAccessException | ParseException e) {
                    e.printStackTrace();
                }
            }
        });


    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("Timeout: " + input);
    }


}
