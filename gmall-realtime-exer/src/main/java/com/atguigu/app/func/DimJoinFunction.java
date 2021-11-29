package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

public interface DimJoinFunction<T> {

    // 获取key
    String getKey(T input);

    // 合并属性方法
    void join(T input, JSONObject dimInfo) throws ParseException;

}
