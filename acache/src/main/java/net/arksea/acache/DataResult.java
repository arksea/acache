package net.arksea.acache;

import java.io.Serializable;

/**
 *
 * Created by arksea on 2016/11/17.
 */
public class DataResult<TKey,TData> implements Serializable {
    public final String cacheName;
    public final TKey key;
    public final TData data;
    public final long expiredTime;
    public final Throwable failed;
    public DataResult(String cacheName, TKey key, TimedData<TData> timedData) {
        this.cacheName = cacheName;
        this.key = key;
        this.data = timedData.data;
        this.failed = null;
        this.expiredTime = timedData.time;
    }
    public DataResult(String cacheName, TKey key, TData data, long expiredTime) {
        this.cacheName = cacheName;
        this.key = key;
        this.data = data;
        this.failed = null;
        this.expiredTime = expiredTime;
    }
    public DataResult(Throwable ex, String cacheName, TKey key) {
        this.cacheName = cacheName;
        this.key = key;
        this.failed = ex;
        this.data = null;
        this.expiredTime = 0;
    }
}
