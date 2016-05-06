package com.kenlai.MKLRedis;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

/**
 * TreeSet sorted by member score and hashed by member value, useful for
 * changing member score, etc.
 */
public class HashTreeSet extends TreeSet<ScoredMember> {

    private static final long serialVersionUID = 1L;

    private Map<String, ScoredMember> hashMap = new HashMap<>();

    @Override
    public boolean add(ScoredMember e) {
        ScoredMember existingMember = hashMap.get(e.getMember());
        if (existingMember != null) {
            if (existingMember.getScore() == e.getScore()) {
                return false;
            }
            super.remove(existingMember);
        }
        hashMap.put(e.getMember(), e);
        return super.add(e) && existingMember != null;
    }

    public boolean removeByMember(String value) {
        ScoredMember remove = hashMap.remove(value);
        if (remove != null) {
            return super.remove(remove);
        }
        return false;
    }
}
