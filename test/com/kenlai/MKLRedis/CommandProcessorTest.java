package com.kenlai.MKLRedis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class CommandProcessorTest {
    private static final String OK = "OK";
    private static final String ERROR = "ERROR";

    private CachingStore store = new CachingStore(16);
    private CommandProcessor cp = new CommandProcessor(store);

    @Test
    public void testProcess() {
        assertEquals(OK, cp.process("SET foo bar"));
        assertTrue(cp.process("BAD").startsWith(ERROR));
        assertTrue(cp.process("What+is&this?").startsWith(ERROR));
        assertEquals("(nil)", cp.process("GET bar"));

        assertEquals("1", cp.process("INCR numerical"));
        assertEquals("2", cp.process("DBSIZE"));

        assertEquals("bar", cp.process("GET foo"));
        assertEquals("1", cp.process("DEL foo"));
        assertEquals("0", cp.process("DEL foo"));
        assertEquals("1", cp.process("DBSIZE"));

        assertEquals("OK", cp.process("ZADD set 10 ten"));
        assertEquals("2", cp.process("DBSIZE"));
        assertEquals("1", cp.process("ZCARD set"));
        assertEquals("OK", cp.process("ZADD set 10 tenB"));
        assertEquals("1", cp.process("ZRANK set tenB"));
        assertEquals("tenB\n", cp.process("ZRANGE set -1 -1"));

        assertTrue(cp.process("GET set").startsWith(ERROR));
        assertTrue(cp.process("INCR set").startsWith(ERROR));

        assertEquals("1", cp.process("DEL set"));
        assertEquals("1", cp.process("DBSIZE"));
    }

}
