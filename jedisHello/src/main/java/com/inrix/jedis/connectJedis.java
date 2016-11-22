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
 * Created by li on 11/19/16.
 */
public class connectJedis {

    public static void main(String[] args) {
        Jedis jedis = new Jedis("localhost");

        jedis.del("keykey2");
        jedis.rpush("keykey2", "value!");


        System.out.println(jedis.lrange("keykey2" , 0, -1).size());



        File nasFile = new File("/Users/li/Documents/learning/smallset2.txt");

        try (BufferedReader br = new BufferedReader(new FileReader(nasFile))) {
            String line;
            int count = 0;
            while ((line = br.readLine()) != null) {

                String rowKey = "1501-" + line.split("\t")[1];
                // case 1 : change key
                System.out.println(rowKey);

                count++;
                if(count == 10){
                    break;
                }
            }
        } catch (Exception ex){

        }

        JedisPool pool = new JedisPool(new JedisPoolConfig(), "localhost");

        FileLoader testLoader = new FileLoader();
        testLoader.loadIntoRedis(pool,
                "/Users/li/Documents/learning/smallset2.txt",
                "1602");

        pool.close();

        List<String> testResult = jedis.lrange("1602-1" , 0, -1);
        List<String> testResult1 = jedis.lrange("1602-2" , 0, -1);
        List<String> testResult2 = jedis.lrange("1602-3", 0, -1);

        jedis.close();

        System.out.println(testResult.size());
    }
}
