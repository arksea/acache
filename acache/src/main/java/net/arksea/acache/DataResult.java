package net.arksea.acache;

import java.io.Serializable;

/**
 *
 * Created by arksea on 2016/11/17.
 */
public class DataResult<TKey,TData> implements Serializable {
    public final String cacheName;
    public final TKey key;
    public final TimedData<TData> timedData;
    public final Throwable failed;
    public DataResult(String cacheName, TKey key, TimedData<TData> timedData) {
        this.cacheName = cacheName;
        this.key = key;
        this.timedData = timedData;
        this.failed = null;
    }
    public DataResult(Throwable ex, String cacheName, TKey key) {
        this.cacheName = cacheName;
        this.key = key;
        this.failed = ex;
        this.timedData = null;
    }
}
