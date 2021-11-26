package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.DimUtil;
import com.atguigu.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements JoinDimFunction<T> {

    // 声明线程池对象
    private ThreadPoolExecutor threadPoolExecutor;

    // 声明属性
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化线程池
        threadPoolExecutor = ThreadPoolUtil.getThreadPoolExecutor();
    }


    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.submit(new Runnable() {
            @Override
            public void run() {

                // 1.获取查询条件
                String id = getId(input);

                // 2.查询维度信息
                JSONObject dimInfo = DimUtil.getDimInfo(tableName, id);

                // 3.关联到事实数据上
                if (dimInfo != null && dimInfo.size() > 0) {
                    try {
                        join(input, dimInfo);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                // 3.继续向下游传输
                resultFuture.complete(Collections.singleton(input));
            }
        });
    }
}
