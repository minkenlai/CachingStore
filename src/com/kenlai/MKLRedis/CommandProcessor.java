package com.kenlai.MKLRedis;

import java.util.List;
import java.util.regex.Pattern;

public class CommandProcessor {
    private boolean verbose = Boolean.getBoolean("verbose");

    private final static Pattern validatorPattern =
            Pattern.compile("\\A[ a-zA-Z0-9-_]*\\z");

    private CachingStore store;

    public CommandProcessor(CachingStore store) {
        this.store = store;
    }

    /**
     * Parses the full command string and forwards to appropriate method.
     *
     * @return result value of the command
     */
    public String process(String request) {
        if (request.isEmpty()) {
            return null;
        }
        if (!validatorPattern.matcher(request).matches()) {
            verbosePrintln("invalid input characters detected");
            return "ERROR invalid input characters detected";
        }
        String[] tokens = request.split(" ");
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
                return store.set(tokens[1], tokens[2], expiration);
            case GET:
                verifyLength(tokens, 2);
                String value = store.get(tokens[1]);
                return (value == null) ? "(nil)" : value;
            case INCR:
                verifyLength(tokens, 2);
                return store.incr(tokens[1]).toString();
            case DEL:
                verifyLength(tokens, 2);
                return Integer.toString(store.del(tokens[1]));
            case DBSIZE:
                verifyLength(tokens, 1);
                return Integer.toString(store.dbsize());
            case ZADD:
                verifyLength(tokens, 4);
                return store.zadd(tokens[1], Long.parseLong(tokens[2]), tokens[3]);
            case ZCARD:
                verifyLength(tokens, 2);
                return Integer.toString(store.zcard(tokens[1]));
            case ZRANK:
                verifyLength(tokens, 3);
                return Integer.toString(store.zrank(tokens[1], tokens[2]));
            case ZRANGE:
                verifyLength(tokens, 4);
                return listToString(store.zrange(tokens[1],
                        Integer.parseInt(tokens[2]),
                        Integer.parseInt(tokens[3])));
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

    private String listToString(List<String> list) {
        if (list.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (String s : list) {
            sb.append(s);
            sb.append(" ");
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    private void verbosePrintln(String msg) {
        if (verbose) {
            System.out.println(msg);
        }
    }
}
