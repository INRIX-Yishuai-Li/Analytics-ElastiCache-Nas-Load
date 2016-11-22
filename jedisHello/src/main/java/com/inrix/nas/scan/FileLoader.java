package com.inrix.nas.scan;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.*;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * Created by li on 11/19/16.
 */
public class FileLoader {

    public void loadIntoRedis (
            JedisPool pool,
            String file,
            String mapVersion
    ) {

        File nasFile = new File(file);

        try (BufferedReader br = new BufferedReader(new FileReader(nasFile))) {

            String line;

            String currentKey = "";
            String[] currentValue = new String[960];

            try (Jedis jedis = pool.getResource()) {


                while ((line = br.readLine()) != null) {
                    String[] afterSplit = line.split("\t");

                    String rowKey = mapVersion + "-" + afterSplit[0];

                    // case 0 : change key (or the first row)
                    if (!rowKey.equals(currentKey)) {
                        if (!currentKey.equals("")) {
                            // First, clean existing data list
                            jedis.del(currentKey);

                            // Second, load list into redis
                            for (String nas : currentValue) {
                                jedis.rpush(currentKey, nas);
                            }
                        }

                        currentKey = rowKey;
                        currentValue = jedis.lrange(rowKey, 0, -1).toArray(new String[0]);
                    }

                    // case 1 : redis side is empty
                    if (currentValue.length < 960) {
                        currentValue = new String[960];
                        Arrays.fill(currentValue, "-1");
                    }

                    currentValue[Integer.parseInt(afterSplit[1])] = afterSplit[2];

                }
            }
        } catch (IOException ioEx) {

        }
    }
}
