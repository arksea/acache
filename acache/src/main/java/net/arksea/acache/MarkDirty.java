package net.arksea.acache;

/**
 * 标记脏数据
 * Created by xiaohaixing on 2017/6/21.
 */
public class MarkDirty<TKey,TData> extends CacheRequest<TKey,TData> {
    public MarkDirty(TKey key) {
        super(key);
    }
}