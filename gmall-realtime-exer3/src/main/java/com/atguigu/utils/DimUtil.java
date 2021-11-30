package com.atguigu.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.List;

public class DimUtil {

    public static JSONObject getDimInfo(Connection connection, String tableName, String key) throws Exception {

        // 先查询Redis
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + "key";
        String jsonStr = jedis.get(redisKey);

        if (jsonStr != null) {

            // 重置过期时间
            jedis.expire(key, 24 * 60 * 60);
            // 归还连接
            jedis.close();
            // 返回值
            return JSONObject.parseObject(jsonStr);
        }

        // 构建查询语句
        StringBuilder selectSql = new StringBuilder("select * from ")
                .append(GmallConfig.HBASE_SCHEMA)
                .append(".")
                .append(tableName)
                .append(" where id = '")
                .append(key)
                .append("'");

        List<JSONObject> list = JdbcUtil.queryList(connection, selectSql.toString(), JSONObject.class, false);

        // 将查到的数据缓存到redis
        jedis.set(key,list.get(0).toJSONString());

        // 重置过期时间
        jedis.expire(key, 24 * 60 * 60);

        // 归还连接
        jedis.close();

        return list.get(0);
    }

    public static void deleteDiminfo(String tableName, String key) {

        // 获取连接
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + "key";
        jedis.del(redisKey);

        jedis.close();
    }

}
