package com.kenlai.MKLRedis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Basic key-value store with expiration and sorted set support.
 */
public class CachingStore {
    private boolean verbose = Boolean.getBoolean("verbose");
    private boolean debug = Boolean.getBoolean("debug");
    private int initialSize = Integer.getInteger("initialSize", 16);

    private static final String OK = "OK";

    private final static Pattern validatorPattern =
            Pattern.compile("\\A[ a-zA-Z0-9-_]*\\z");
    private final static Pattern integerPattern =
            Pattern.compile("\\A[+-]?[0-9]+\\z");

    private HashMap<String, Object> map;

    private List<String> expirables = new LinkedList<>();

    public CachingStore() {
        map = new HashMap<String, Object>(initialSize);
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
            verbosePrintln("invalid input characters detected");
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
                    verbosePrintln("incorrect parameters for SET");
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
                verbosePrintln("Command " + tokens[0]
                        + " is not yet implemented");
            }
        } catch (IllegalArgumentException e) {
            verbosePrintln("bad command: " + tokens[0]);
            return "ERROR bad command";
        } catch (IndexOutOfBoundsException e) {
            verbosePrintln("incorrect number of parameters");
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
     * Get the value of key. If the key does not exist null is
     * returned. An error is returned if the value stored at key is not a
     * string, because GET only handles string values.
     *
     * @return the value of key, or null when key does not exist
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

    /**
     * Adds the specified member with the specified score to the sorted set
     * stored at key. If a specified member is already a member of the sorted
     * set, the score is updated and the element reinserted at the right
     * position to ensure the correct ordering.
     */
    public String zadd(String key, long score, String member) {
        HashTreeSet sortedSet = null;
        Object value = map.get(key);
        if (value == null) {
            sortedSet = new HashTreeSet();
            map.put(key, sortedSet);
        } else if (value instanceof HashTreeSet) {
            sortedSet = (HashTreeSet) value;
        } else {
            throw new IllegalArgumentException("value is incorrect type");
        }
        if (!sortedSet.add(new ScoredMember(score, member))) {
            verbosePrintln("replaced member: " + member);
        }
        return OK;
    }

    /**
     * @return the sorted set cardinality (number of elements) of the sorted set
     *         stored at key.
     */
    public int zcard(String key) {
        Object value = map.get(key);
        if (value == null) {
            return 0;
        }
        if (value instanceof HashTreeSet) {
            HashTreeSet sortedSet = (HashTreeSet) value;
            return sortedSet.size();
        }
        throw new IllegalArgumentException("value is incorrect type");
    }

    /**
     * Returns the rank of member in the sorted set stored at key, with the
     * scores ordered from low to high. The rank (or index) is 0-based, which
     * means that the member with the lowest score has rank 0.
     *
     * @param key key to retrieve sorted set
     * @param member value to seek in sorted set
     * @return 0-based index in sorted set; null if not found
     */
    public Integer zrank(String key, String member) {
        Object value = map.get(key);
        if (value instanceof HashTreeSet) {
            HashTreeSet sortedSet = (HashTreeSet) value;
            int rank = 0;
            for (ScoredMember m : sortedSet) {
                if (m.member.equals(member)) {
                    return rank;
                }
                rank++;
            }
            return null;
        }
        throw new IllegalArgumentException("value is incorrect type");
    }

    /**
     * Returns the specified range of elements in the sorted set stored at key.
     * The elements are considered to be ordered from the lowest to the highest
     * score. Lexicographical order is used for elements with equal score.
     * <p>
     * Indices can also be negative numbers indicating offsets from the end of
     * the sorted set, with -1 being the last element of the sorted set.
     *
     * @param key key to retrieve sorted set
     * @param start 0-based index, inclusive
     * @param stop 0-based index, inclusive
     * @return list of member values
     */
    public List<String> zrange(String key, int start, int stop) {
        Object value = map.get(key);
        if (value == null) {
            return Collections.emptyList();
        }
        if (value instanceof HashTreeSet) {
            HashTreeSet sortedSet = (HashTreeSet) value;
            int size = sortedSet.size();
            int begin = start < 0 ? size + start : start;
            int end = stop < 0 ? size + stop : stop;
            if (begin >= size || end < begin) {
                return Collections.emptyList();
            }
            if (end >= size) {
                end = size - 1;
            }
            if (debug) {
                assert(begin >=0 && end < size);
            }
            verbosePrintln("begin=" + begin + " end=" + end);
            Iterator<ScoredMember> iterator = sortedSet.iterator();
            int i = 0;
            while (i < begin) {
                iterator.next();
                i++;
            }
            List<String> list = new ArrayList<>(end - begin + 1);
            while (i <= end) {
                list.add(iterator.next().getMember());
                i++;
            }
            return list;
        }
        throw new IllegalArgumentException("value is incorrect type");
    }

    private void verbosePrintln(String msg) {
        if (verbose) {
            System.out.println(msg);
        }
    }
}
