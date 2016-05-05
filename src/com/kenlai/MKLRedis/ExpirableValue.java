package com.kenlai.MKLRedis;

/**
 * ExpirableValue is a wrapper object used only in cases where expiration of
 * the stored value is needed. This way we don't incur extra object for
 * every value.
 */
class ExpirableValue {
    private long expiresAt;
    Object value;

    public ExpirableValue(Object value, long expiresAt) {
        this.value = value;
        this.expiresAt = expiresAt;
    }

    public boolean isExpired() {
        // TODO: decide if we want the "same ms" to be expired or not.
        return System.currentTimeMillis() > expiresAt;
    }

    public String toString() {
        return value.toString();
    }

    public int hashCode() {
        int hash = super.hashCode();
        hash = 89 * hash + value.hashCode();
        hash = 89 * hash + (int) expiresAt;
        return hash;
    }
}