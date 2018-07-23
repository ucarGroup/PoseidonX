
package com.ucar.streamsuite.common.util;

public interface ExpiredCallback<K, V> {
    public void expire(K key, V val);
}
