package net.arksea.acache;

import akka.routing.ConsistentHashingRouter;

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
        if (key instanceof ConsistentHashingRouter.ConsistentHashable) {
            return ((ConsistentHashingRouter.ConsistentHashable) key).consistentHashKey();
        } else {
            return key;
        }
    }
}