package com.atguigu.app.func;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

public abstract class  DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements JoinDimFunction<T>{


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {

        String key = getKey(input);


    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        super.timeout(input, resultFuture);
    }
}
