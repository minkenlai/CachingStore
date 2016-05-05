package com.kenlai.MKLRedis;

public class ScoredMember implements Comparable<ScoredMember> {
    long score;
    String member;

    public ScoredMember(long score, String member) {
        this.score = score;
        this.member = member;
    }

    public long getScore() {
        return score;
    }
    public String getMember() {
        return member;
    }

    public int compareTo(ScoredMember o) {
        int result = Long.compare(score, o.score);
        if (result == 0) {
            result = member.compareTo(o.member);
        }
        return result;
    }
}
