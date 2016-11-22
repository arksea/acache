package net.arksea.acache;

/**
 *
 * Created by arksea on 2016/11/17.
 */
public class DataResult<TKey,TData> {
    public final String cacheName;
    public final TKey key;
    public final TData data;
    public final Exception failed;
    public final boolean sync;
    public DataResult(String cacheName, TKey key, TData data) {
        this.cacheName = cacheName;
        this.key = key;
        this.data = data;
        this.failed = null;
        this.sync = false;
    }
    public DataResult(String cacheName, TKey key, TData data, boolean sync) {
        this.cacheName = cacheName;
        this.key = key;
        this.data = data;
        this.failed = null;
        this.sync = sync;
    }
    public DataResult(String cacheName, TKey key, Exception ex) {
        this.cacheName = cacheName;
        this.key = key;
        this.failed = ex;
        this.data = null;
        this.sync = false;
    }
}
