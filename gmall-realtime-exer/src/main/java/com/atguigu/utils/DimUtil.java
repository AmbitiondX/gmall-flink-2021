package com.atguigu.utils;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

public class DimUtil {
    public static JSONObject getDimInfo(Connection connection, String tableName, String key) throws SQLException, InvocationTargetException, InstantiationException, IllegalAccessException {

        // 先查询redis数据
        String redisKey = "DIM:" + tableName + ":" + key;
        Jedis jedis = RedisUtil.getJedis();
        String dimInfoInRedis = jedis.get(redisKey);
        if (dimInfoInRedis != null) {
            // 重置过期时间
            jedis.expire(redisKey, 60 * 60 * 24);
            // 归还连接
            jedis.close();
            // 返回结果数据
            return JSON.parseObject(dimInfoInRedis);
        }

        // 构建查询语句
        String querySql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id='" + key + "'";

        List<JSONObject> queryList = JdbcUtil.queryList(connection, querySql, JSONObject.class, false);

        // 将数据写入redis缓存
        jedis.set(redisKey,queryList.get(0).toJSONString());

        // 设置过期时间
        jedis.expire(redisKey, 60 * 60 * 24);
        // 归还连接
        jedis.close();
        // 返回结果数据
        return queryList.get(0);

    }

    // 删除Redis中数据
    public static void deleteDimInfo(String tableName, String key) {
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + key;
        jedis.del(redisKey);
        jedis.close();
    }


    public static void main(String[] args) throws Exception {

        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        long start = System.currentTimeMillis();
        System.out.println(getDimInfo(connection, "DIM_BASE_TRADEMARK", "12"));  //240
        long end = System.currentTimeMillis();
        System.out.println(getDimInfo(connection, "DIM_BASE_TRADEMARK", "12"));  //10
        long end2 = System.currentTimeMillis();

        System.out.println(end - start);
        System.out.println(end2 - end);

        connection.close();

    }



}
