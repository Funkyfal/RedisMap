package org.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.*;

public class RedisMap implements Map<String, String> {
    private final JedisPool jedisPool;
    private final int redisDbIndex;

    public RedisMap(JedisPool jedisPool, int redisDbIndex) {
        this.jedisPool = jedisPool;
        this.redisDbIndex = redisDbIndex;
    }

    private Jedis getJedis() {
        Jedis jedis = jedisPool.getResource();
        jedis.select(redisDbIndex);
        return jedis;
    }

    @Override
    public int size() {
        try (Jedis jedis = getJedis()) {
            return Math.toIntExact(jedis.dbSize());
        }
    }

    @Override
    public boolean isEmpty() {
        try (Jedis jedis = getJedis()) {
            return jedis.dbSize() == 0;
        }
    }

    @Override
    public boolean containsKey(Object key) {
        if (!(key instanceof String)) {
            return false;
        }
        try (Jedis jedis = getJedis()) {
            return jedis.exists((String) key);
        }
    }

    //Неэффективно
    @Override
    public boolean containsValue(Object value) {
        if (!(value instanceof String)) {
            return false;
        }
        try (Jedis jedis = getJedis()) {
            Set<String> keys = jedis.keys("*");
            for (String key : keys) {
                String v = jedis.get(key);
                if (v.equals(value)) {
                    return true;
                }
            }
            return false;
        }
    }

//    @Override
//    public boolean containsValue(Object value) {
//        if (!(value instanceof String)) {
//            return false;
//        }
//        String targetValue = (String) value;
//
//        try (Jedis jedis = jedisPool.getResource()) {
//            String cursor = "0";
//            ScanParams scanParams = new ScanParams().count(100); // Чтение по 100 записей за итерацию
//
//            do {
//                ScanResult<Map.Entry<String, String>> scanResult =
//                        jedis.hscan(mapKey, cursor, scanParams);
//
//                for (Map.Entry<String, String> entry : scanResult.getResult()) {
//                    if (targetValue.equals(entry.getValue())) {
//                        return true;
//                    }
//                }
//
//                cursor = scanResult.getCursor();
//            } while (!"0".equals(cursor)); // Продолжаем пока курсор не вернется в 0
//
//            return false;
//        }
//    }

    @Override
    public String get(Object key) {
        if (!(key instanceof String)) {
            return null;
        }
        try (Jedis jedis = getJedis()) {
            return jedis.get((String) key);
        }
    }

    @Override
    public String put(String key, String value) {
        try (Jedis jedis = getJedis()) {
            String oldValue = jedis.get(key);
            jedis.set(key, value);
            return oldValue;
        }
    }

    @Override
    public String remove(Object key) {
        if (!(key instanceof String)) {
            return null;
        }
        try (Jedis jedis = getJedis()) {
            String oldValue = jedis.get((String) key);
            jedis.del((String) key);
            return oldValue;
        }
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
        if (m == null || m.isEmpty()) {
            return;
        }

        try (Jedis jedis = getJedis()) {
            List<String> kvList = new ArrayList<>(m.size() * 2);
            for (Entry<? extends String, ? extends String> e : m.entrySet()) {
                kvList.add(e.getKey());
                kvList.add(e.getValue());
            }
            jedis.mset(kvList.toArray(new String[0]));
        }
    }

    @Override
    public void clear() {
        try (Jedis jedis = getJedis()) {
            jedis.flushDB();
        }
    }

    @Override
    public Set<String> keySet() {
        try (Jedis jedis = getJedis()) {
            return jedis.keys("*");
        }
    }

    @Override
    public Collection<String> values() {
        try (Jedis jedis = getJedis()) {
            Set<String> keys = jedis.keys("*");
            List<String> values = new ArrayList<>(keys.size());
            for (String key : keys) {
                values.add(jedis.get(key));
            }
            return values;
        }
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        try (Jedis jedis = getJedis()) {
            Set<String> keys = jedis.keys("*");
            Set<Entry<String, String>> result = new HashSet<>();
            for (String key : keys){
                String value = jedis.get(key);
                result.add(new AbstractMap.SimpleEntry<>(key, value));
            }
            return result;
        }
    }
}
