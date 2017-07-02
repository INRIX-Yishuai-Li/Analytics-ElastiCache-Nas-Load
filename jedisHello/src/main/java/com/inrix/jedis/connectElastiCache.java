package com.inrix.jedis;

import com.inrix.nas.scan.FileLoader;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.List;

/**
 * Created by Yishuai.Li on 11/21/2016.
 */
public class connectElastiCache {

    public static void main(String[] args) {
        Jedis jedis = new Jedis("nastestredis.napkrz.0001.usw2.cache.amazonaws.com", 6379);

        System.out.println(jedis.lrange("1602-1002905519", 0, -1).size());

        //jedis.del("keykey2");
        //jedis.rpush("keykey2", "value!");



        /*JedisPoolConfig config = new JedisPoolConfig();
        JedisPool pool = new JedisPool(new JedisPoolConfig(), "nasredistest.napkrz.ng.0001.usw2.cache.amazonaws.com");

        try (Jedis jedis = pool.getResource()) {
            jedis.del("keykey2");
            jedis.rpush("keykey2", "value!");


            System.out.println(jedis.lrange("keykey2" , 0, -1).size());
        }*/
    }
}
