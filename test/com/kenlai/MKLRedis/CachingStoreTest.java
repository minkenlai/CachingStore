package com.kenlai.MKLRedis;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.List;

import org.junit.Test;

public class CachingStoreTest {
    private static final String OK = "OK";

    @Test
    public void testCachingStore() throws Exception {
        CachingStore store = new CachingStore(16);

        // CASE: SET, GET, EX, DBSIZE
        assertEquals(OK, store.set("A", "A", 1L));
        assertEquals(OK, store.set("B", "B", null));
        assertEquals("A", store.get("A"));
        assertEquals("B", store.get("B"));
        assertEquals(2, store.dbsize());
        Thread.sleep(2000L);
        assertNull(store.get("A"));
        assertEquals("B", store.get("B"));
        assertEquals(1, store.dbsize());

        // CASE: INCR type checking
        try {
            store.incr("B");
            fail("should not be able to INCR string type");
        } catch (IllegalArgumentException e) {
            // expected
        }

        // CASE: SET over-write, INCR value with expiration
        assertEquals(OK, store.set("B", "3", 1L));
        assertEquals("4", store.incr("B").toString());
        assertEquals(1, store.dbsize());
        Thread.sleep(2000L);
        assertNull(store.get("B"));
        assertEquals(0, store.dbsize());

        // CASE: INCR from nothing
        assertEquals("1", store.incr("B").toString());
        assertEquals(1, store.dbsize());
    }

    @Test
    public void testGarbageCollection() throws Exception {
        for (int count = 1; count <= 10000; count = 10 * count) {
            CachingStore store = new CachingStore(count);
            long before = System.currentTimeMillis();
            for (int i = 0; i < count; i++) {
                store.set("A" + i, "Not Expiring", null);
                store.set("B" + i, "Expiring First", 1L);
                store.set("C" + i, "Expiring Even Later", 6000L);
                store.set("D" + i, "Expiring Second", 2L);
                store.set("E" + i, "Expiring Later", 4500L);
            }
            long after = System.currentTimeMillis();
            int totalCreated = store.dbsize();
            System.out.println("Created " + totalCreated + " entries in "
                    + Long.toString(after - before) + "ms");
            assertEquals(count * 5, totalCreated);
            assertEquals(totalCreated, store.dbsize()); // should not expire yet
            // Let half of the entries expire
            System.out.println("sleeping 3s to allow expiration");
            Thread.sleep(3000L);
            before = System.currentTimeMillis();
            // GC happens when we try to get dbsize
            int remaining = store.dbsize();
            after = System.currentTimeMillis();
            System.out.println("Garbage collected "
                    + Long.toString(totalCreated - remaining) + " entries in "
                    + Long.toString(after - before) + "ms");
            assertEquals(count * 3, remaining);
        }
    }

    @Test
    public void testSortedSet() {
        CachingStore store = new CachingStore(16);
        // CASE: not-exist / empty
        assertEquals(0, store.zcard("Z"));
        List<String> list = store.zrange("Z", 0, 0);
        assertEquals(0, list.size());

        // CASE: single element
        assertEquals(OK, store.zadd("Z", 10, "ten"));
        assertEquals(1, store.zcard("Z"));
        assertEquals(0, (int) store.zrank("Z", "ten"));

        list = store.zrange("Z", 0, 0);
        assertEquals(1, list.size());
        assertEquals("ten", list.get(0));
        list = store.zrange("Z", 1, 1);
        assertEquals(0, list.size());

        // CASE: two elements
        assertEquals(OK, store.zadd("Z", 5, "five"));
        assertEquals(2, store.zcard("Z"));
        assertEquals(1, (int) store.zrank("Z", "ten"));
        assertEquals(0, (int) store.zrank("Z", "five"));

        list = store.zrange("Z", 0, 0);
        assertEquals(1, list.size());
        assertEquals("five", list.get(0));
        list = store.zrange("Z", 1, 1);
        assertEquals(1, list.size());
        assertEquals("ten", list.get(0));
        list = store.zrange("Z", -1, -1);
        assertEquals(1, list.size());
        assertEquals("ten", list.get(0));

        list = store.zrange("Z", 0 , -1);
        assertEquals(2, list.size());
        assertEquals("five", list.get(0));
        assertEquals("ten", list.get(1));

        // CASE: three elements
        assertEquals(OK, store.zadd("Z", 5, "fiveB"));
        assertEquals(3, store.zcard("Z"));
        assertEquals(0, (int) store.zrank("Z", "five"));
        assertEquals(1, (int) store.zrank("Z", "fiveB"));
        assertEquals(2, (int) store.zrank("Z", "ten"));

        list = store.zrange("Z", 1, 1);
        assertEquals(1, list.size());
        assertEquals("fiveB", list.get(0));

        // CASE: change score
        assertEquals(OK, store.zadd("Z", 10, "variable"));
        assertEquals(4, store.zcard("Z"));
        assertEquals(3, (int) store.zrank("Z", "variable"));

        list = store.zrange("Z", 0, -1);
        Object[] expected = new Object[] {"five", "fiveB", "ten", "variable"};
        assertArrayEquals(expected, list.toArray());

        assertEquals(OK, store.zadd("Z", 5, "variable"));
        assertEquals(2, (int) store.zrank("Z", "variable"));

        list = store.zrange("Z", 0, -1);
        expected = new Object[] {"five", "fiveB", "variable", "ten"};
        assertArrayEquals(expected, list.toArray());
    }
}
