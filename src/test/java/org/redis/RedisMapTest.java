package org.redis;

import org.junit.jupiter.api.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RedisMapTest {
    private RedisMap redisMap;
    private JedisPool jedisPool;
    private static final int TEST_DB = 1;

    @BeforeAll
    void setupPool(){
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(5);
        jedisPool = new JedisPool(jedisPoolConfig, "localhost", 6379);
        redisMap = new RedisMap(jedisPool, TEST_DB);
    }

    @BeforeEach
    void cleanDatabase(){
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.select(TEST_DB);
            jedis.flushDB();
        }
    }

    @AfterAll
    void tearDownPool() {
        if (jedisPool != null) {
            jedisPool.close();
        }
    }

    @Test
    void testPutAndGet() {
        assertTrue(redisMap.isEmpty());

        String prev = redisMap.put("key1", "value1");
        assertNull(prev, "prevValue must me null for a new key");

        String val = redisMap.get("key1");
        assertEquals("value1", val);

        String old = redisMap.put("key1", "value2");
        assertEquals("value1", old, "prevValue must be old value");

        assertEquals("value2", redisMap.get("key1"));
        assertFalse(redisMap.isEmpty());
        assertEquals(1, redisMap.size());
        assertTrue(redisMap.containsKey("key1"));
        assertTrue(redisMap.containsValue("value2"));
    }

    @Test
    void testRemove() {
        redisMap.put("a", "alpha");
        redisMap.put("b", "beta");

        assertEquals(2, redisMap.size());

        String removed = redisMap.remove("a");
        assertEquals("alpha", removed);
        assertFalse(redisMap.containsKey("a"));
        assertEquals(1, redisMap.size());

        String nothing = redisMap.remove("noKey");
        assertNull(nothing);
        assertEquals(1, redisMap.size());
    }

    @Test
    void testPutAllAndClear() {
        Map<String, String> m = new HashMap<>();
        m.put("x", "X");
        m.put("y", "Y");
        m.put("z", "Z");

        redisMap.putAll(m);
        assertEquals(3, redisMap.size());
        assertTrue(redisMap.containsKey("x"));
        assertEquals("Y", redisMap.get("y"));

        redisMap.clear();
        assertTrue(redisMap.isEmpty());
        assertEquals(0, redisMap.size());
    }

    @Test
    void testKeySetAndValuesAndEntrySet() {
        redisMap.put("k1", "v1");
        redisMap.put("k2", "v2");

        Set<String> ks = redisMap.keySet();
        assertEquals(2, ks.size());
        assertTrue(ks.contains("k1"));
        assertTrue(ks.contains("k2"));

        Collection<String> vals = redisMap.values();
        assertEquals(2, vals.size());
        assertTrue(vals.contains("v1"));
        assertTrue(vals.contains("v2"));

        Set<Map.Entry<String, String>> entries = redisMap.entrySet();
        assertEquals(2, entries.size());
        Map<String, String> asMap = new HashMap<>();
        for (Map.Entry<String, String> e : entries) {
            asMap.put(e.getKey(), e.getValue());
        }
        assertEquals("v1", asMap.get("k1"));
        assertEquals("v2", asMap.get("k2"));
    }

    @Test
    void testContainsValuePerformanceNote() {
        redisMap.put("foo", "bar");
        redisMap.put("hello", "world");

        assertTrue(redisMap.containsValue("bar"));
        assertFalse(redisMap.containsValue("nonexistent"));
    }
}
