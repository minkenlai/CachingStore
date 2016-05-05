package com.kenlai.MKLRedis;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;
import java.util.regex.Pattern;

/*
 SET key value
 SET key value EX seconds (need not implement other SET options)
 GET key
 DEL key
 DBSIZE
 INCR key
 ZADD key score member
 ZCARD key
 ZRANK key member
 ZRANGE key start stop
 */

public class CachingStore {
    private static final String OK = "OK";

    private final static Pattern validatorPattern = Pattern
            .compile("\\A[ a-zA-Z0-9-_]*\\z");
    private final static Pattern integerPattern = Pattern
            .compile("\\A[+-]?[0-9]+\\z");

    private HashMap<String, Object> map;

    private List<String> expirables = new LinkedList<>();

    public CachingStore() {
        // TODO: look into sensible and/or configurable initial sizes
        map = new HashMap<String, Object>();
    }

    public CachingStore(int size) {
        map = new HashMap<String, Object>(size);
    }

    /**
     * Parses the full command string and forwards to appropriate method.
     *
     * @return result value of the command
     */
    public String process(String fullCmd) {
        if (!validatorPattern.matcher(fullCmd).matches()) {
            System.out.println("invalid input characters detected");
            return "ERROR invalid input characters detected";
        }
        String[] tokens = fullCmd.split(" ");
        try {
            Command cmd = Command.valueOf(tokens[0]);
            switch (cmd) {
            case SET:
                Long expiration = null;
                if (tokens.length == 5 && tokens[3].equals("EX")) {
                    expiration = Long.getLong(tokens[4]);
                } else if (tokens.length != 3) {
                    System.out.println("incorrect parameters for SET");
                    return "ERROR bad SET parameters";
                }
                return set(tokens[1], tokens[2], expiration);
            case GET:
                verifyLength(tokens, 2);
                return get(tokens[1]);
            case INCR:
                verifyLength(tokens, 2);
                return incr(tokens[1]).toString();
            case DEL:
                verifyLength(tokens, 2);
                return Integer.toString(del(tokens[1]));
            case DBSIZE:
                verifyLength(tokens, 1);
                return Integer.toString(dbsize());
            default:
                System.out.println("Command " + tokens[0]
                        + " is not yet implemented");
            }
        } catch (IllegalArgumentException e) {
            System.out.println("bad command: " + tokens[0]);
            return "ERROR bad command";
        } catch (IndexOutOfBoundsException e) {
            System.out.println("incorrect number of parameters");
            return "ERROR number of parameters";
        }
        return null;
    }

    private void verifyLength(String[] tokens, int length) {
        if (tokens.length != length) {
            throw new IllegalArgumentException("incorrect number of parameters");
        }
    }

    /**
     * Note: this has side-effect of garbage collecting expired entries, O(n).
     *
     * @return number of keys
     */
    public int dbsize() {
        garbageCollect();
        return map.size();
    }

    /**
     * TODO: This is currently an O(n) operation, but can be optimized by using
     * a heap ordered by the expiration time to track the expiring entries.
     *
     * With a PriorityQueue, we would be able to just poll the expired entries,
     * but it incurs O(log(n)) for insertion.
     */
    private void garbageCollect() {
        Iterator<String> iterator = expirables.iterator();
        while (iterator.hasNext()) {
            String key = iterator.next();
            ExpirableValue ev = (ExpirableValue) map.get(key);
            if (ev.isExpired()) {
                iterator.remove();
                map.remove(key);
            }
        }
    }

    /**
     * Get the value of key. If the key does not exist the special value nil is
     * returned. An error is returned if the value stored at key is not a
     * string, because GET only handles string values.
     *
     * @return the value of key, or nil when key does not exist
     */
    public String get(String key) {
        Object value = getUnexpired(key);
        return value != null ? value.toString() : null;
    }

    /**
     * if entry is expired, remove it and return null.
     */
    private Object getUnexpired(String key) {
        Object value = map.get(key);
        if (value instanceof ExpirableValue) {
            ExpirableValue ev = (ExpirableValue) value;
            if (ev.isExpired()) {
                del(key);
                return null;
            }
        }
        return value;
    }

    /**
     * Removes the specified key. A key is ignored if it does not exist.
     *
     * @return number of keys that were removed
     */
    public int del(String key) {
        Object value = map.remove(key);
        if (value == null) {
            return 0;
        }
        // clean from expirables lists
        if (value instanceof ExpirableValue) {
            expirables.remove(key);
        }
        return 1;
    }

    /**
     * Set key to hold the string value. If key already holds a value, it is
     * overwritten, regardless of its type. Any previous time to live associated
     * with the key is discarded on successful SET operation.
     *
     * @param expiration
     *            expire time, in seconds; null if none
     */
    public String set(String key, String value, Long timeToLive) {
        Object val = value;
        if (shouldStoreAsInteger(value)) {
            val = Long.parseLong(value);
        }
        if (timeToLive != null) {
            if (timeToLive <= 0) { // TODO: Is it better to return an error?
                del(key);
                return OK;
            }
            long expiresAt = System.currentTimeMillis() + timeToLive * 1000;
            val = new ExpirableValue(val, expiresAt);
            expirables.add(key);
        }
        map.put(key, val);
        return OK;
    }

    private boolean shouldStoreAsInteger(String value) {
        return integerPattern.matcher(value).matches();
    }

    /**
     * Increments the number stored at key by one. If the key does not exist, it
     * is set to 0 before performing the operation. An error is returned if the
     * key contains a value of the wrong type or contains a string that can not
     * be represented as integer. This operation is limited to 64 bit signed
     * integers.
     * <p>
     * Note: this is a string operation because we do not have a dedicated
     * integer type. The string stored at the key is interpreted as a base-10 64
     * bit signed integer to execute the operation.
     * <p>
     * Integers are stored in their integer representation, so for string values
     * that actually hold an integer, there is no overhead for storing the
     * string representation of the integer.
     *
     * @return value after increment
     */
    public Object incr(String key) {
        Object value = getUnexpired(key);
        if (value == null) {
            value = 0L;
        }

        // Swap in value from expirable, if needed
        ExpirableValue e = null;
        if (value instanceof ExpirableValue) {
            e = (ExpirableValue) value;
            value = e.value;
        }

        if (value instanceof Long) {
            value = (Long) value + 1L;
        } else {
            throw new IllegalArgumentException("value is not integer type");
        }

        // Swap back into expirable, if needed
        if (e != null) {
            e.value = value;
            value = e;
        }

        map.put(key, value);
        return value;
    }


    public String zadd(String key, long score, String member) {
        TreeSet<ScoredMember> sortedSet = null;
        Object value = map.get(key);
        if (value == null) {
            sortedSet = new TreeSet<>();
            map.put(key, sortedSet);
        } else if (value instanceof TreeSet<?>) {
            @SuppressWarnings("unchecked")
            TreeSet<ScoredMember> existing = (TreeSet<ScoredMember>) value;
            sortedSet = existing;
        } else {
            throw new IllegalArgumentException("value is incorrect type");
        }
        sortedSet.add(new ScoredMember(score, member));
        return OK;
    }

    public int zcard(String key) {
        Object value = map.get(key);
        if (value == null) {
            return 0;
        }
        if (value instanceof TreeSet<?>) {
            @SuppressWarnings("unchecked")
            TreeSet<ScoredMember> sortedSet = (TreeSet<ScoredMember>) value;
            return sortedSet.size();
        }
        throw new IllegalArgumentException("value is incorrect type");
    }

    public int zrank(String key, String member) {
        Object value = map.get(key);
        if (value instanceof TreeSet<?>) {
            @SuppressWarnings("unchecked")
            TreeSet<ScoredMember> sortedSet = (TreeSet<ScoredMember>) value;

            // TODO: implement
        }
        throw new IllegalArgumentException("value is incorrect type");
    }

    public int zrange(String key, int start, int stop) {
        Object value = map.get(key);
        if (value instanceof TreeSet<?>) {
            @SuppressWarnings("unchecked")
            TreeSet<ScoredMember> sortedSet = (TreeSet<ScoredMember>) value;
//            sortedSet.subSet(fromElement, fromInclusive, toElement, toInclusive);
        }
        throw new IllegalArgumentException("value is incorrect type");
    }
}
