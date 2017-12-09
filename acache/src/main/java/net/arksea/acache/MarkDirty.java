package net.arksea.acache;

/**
 * 标记脏数据
 * Created by xiaohaixing on 2017/6/21.
 */
public class MarkDirty<TKey,TData> implements ICacheRequest<TKey,TData> {
    public final TKey key;

    public MarkDirty(TKey key) {
        this.key = key;
    }
    public TKey getKey() {
        return key;
    }
    @Override
    public Object consistentHashKey() {
        return key;
    }
}