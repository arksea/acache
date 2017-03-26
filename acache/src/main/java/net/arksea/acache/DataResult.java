package net.arksea.acache;

/**
 *
 * Created by arksea on 2016/11/17.
 */
public class DataResult<TKey,TData> {
    public final String cacheName;
    public final TKey key;
    public final TData data;
    public final Throwable failed;
    public final boolean sync;
    public final boolean newest; //从数据源取新数据失败，或者正在backoff中，返回的是旧的缓存数据
    public DataResult(String cacheName, TKey key, TData data) {
        this.cacheName = cacheName;
        this.key = key;
        this.data = data;
        this.failed = null;
        this.sync = false;
        this.newest = true;
    }
    public DataResult(String cacheName, TKey key, TData data, boolean newest) {
        this.cacheName = cacheName;
        this.key = key;
        this.data = data;
        this.failed = null;
        this.sync = false;
        this.newest = newest;
    }
    public DataResult(String cacheName, TKey key, TData data, boolean newest, boolean sync) {
        this.cacheName = cacheName;
        this.key = key;
        this.data = data;
        this.failed = null;
        this.sync = sync;
        this.newest = newest;
    }
    public DataResult(Throwable ex, String cacheName, TKey key) {
        this.cacheName = cacheName;
        this.key = key;
        this.failed = ex;
        this.data = null;
        this.sync = false;
        this.newest = false;
    }
}
