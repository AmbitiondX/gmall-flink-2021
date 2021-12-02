package com.atguigu.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class DimUtil {

    public static JSONObject getDimInfo(Connection connection, String tableName, String key) throws SQLException, InvocationTargetException, InstantiationException, IllegalAccessException {

        String redisKey = "DIM:" + tableName + ":" + key;
        Jedis jedis = ReidsUtil.getJedis();
        String jsonStr = jedis.get(redisKey);
        if (jsonStr != null) {
            jedis.expire(redisKey, 24 * 60 * 60);
            jedis.close();
            return JSONObject.parseObject(jsonStr);
        }

        String sql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id = '" + key  + "'";
        List<JSONObject> list = JdbcUtil.queryList(connection, sql, JSONObject.class, false);
        JSONObject jsonObject = list.get(0);

        jedis.set(redisKey,jsonObject.toJSONString());
        jedis.expire(redisKey, 24 * 60 * 60);
        jedis.close();

        return jsonObject;
    }

    public static void deleteDimInfo(String tableName, String key) {
        Jedis jedis = ReidsUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + key;
        jedis.del(redisKey);
        jedis.close();
    }

}
