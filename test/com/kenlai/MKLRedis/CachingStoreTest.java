package com.kenlai.MKLRedis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class CachingStoreTest {

    @Test
    public void testProcess() {
        CachingStore store = new CachingStore();
        store.process("SET foo bar");
        store.process("BAD");
        store.process("What+is&this?");
    }

    @Test
    public void testCachingStore() throws Exception {
        CachingStore store = new CachingStore();

        // CASE: SET, GET, EX, DBSIZE
        store.set("A", "A", 1L);
        store.set("B", "B", null);
        assertEquals("A", store.get("A"));
        assertEquals("B", store.get("B"));
        assertEquals(2, store.dbsize());
        Thread.sleep(1000L);
        assertNull(store.get("A"));
        assertEquals("B", store.get("B"));
        assertEquals(1, store.dbsize());

        // CASE: INCR type checking
        try {
            store.incr("B");
        } catch (IllegalArgumentException e) {
            // expected
        }

        // CASE: SET over-write, INCR value with expiration
        store.set("B", "3", 1L);
        assertEquals("4", store.incr("B").toString());
        assertEquals(1, store.dbsize());
        Thread.sleep(1000L);
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
}
