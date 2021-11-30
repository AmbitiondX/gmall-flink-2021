package com.atguigu.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtil {

    private static JedisPool jedisPool;

    public static Jedis getJedis(){

        if (jedisPool == null) {

            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();

            //最大可用连接数
            jedisPoolConfig.setMaxTotal(100);

            //连接耗尽是否等待
            jedisPoolConfig.setBlockWhenExhausted(true);

            //等待时间
            jedisPoolConfig.setMaxWaitMillis(2000);

            //最大闲置连接数
            jedisPoolConfig.setMaxIdle(5);

            //最小闲置连接数
            jedisPoolConfig.setMinIdle(5);

            //取连接的时候进行一下测试 ping pong
            jedisPoolConfig.setTestOnBorrow(true);

            JedisPool jedisPool = new JedisPool(jedisPoolConfig, "hadoop102", 6379,1000);

            return jedisPool.getResource();

        } else{
            return jedisPool.getResource();
        }

    }
}
