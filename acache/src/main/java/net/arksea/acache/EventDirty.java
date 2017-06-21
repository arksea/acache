package net.arksea.acache;

/**
 * 脏数据通知事件
 * Created by xiaohaixing on 2017/6/21.
 */
public class EventDirty<TKey,TData> implements ICacheRequest<TKey,TData> {
    public final TKey key;

    public EventDirty(TKey key) {
        this.key = key;
    }

    @Override
    public Object consistentHashKey() {
        return key;
    }
}