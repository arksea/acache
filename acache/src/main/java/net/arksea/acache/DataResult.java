package net.arksea.acache;

import java.io.Serializable;

/**
 *
 * Created by arksea on 2016/11/17.
 */
public class DataResult<TKey,TData> implements Serializable {
    final public TData data;
    final public long expiredTime;
    public final String cacheName;
    public final TKey key;
    public final Throwable failed;
    public DataResult(String cacheName, TKey key, final long time, final TData data) {
        this.expiredTime = time;
        this.data = data;
        this.cacheName = cacheName;
        this.key = key;
        this.failed = null;
    }
    public DataResult(Throwable ex, String cacheName, TKey key) {
        this.expiredTime = 0;
        this.data = null;
        this.cacheName = cacheName;
        this.key = key;
        this.failed = ex;
    }
}
