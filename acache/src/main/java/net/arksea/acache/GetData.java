package net.arksea.acache;

import akka.routing.ConsistentHashingRouter;

/**
 *
 * Created by arksea on 2016/11/17.
 */
public class GetData<TKey,TData> implements ICacheRequest<TKey,TData> {
    public final TKey key;

    public TKey getKey() {
        return key;
    }

    public GetData(TKey key) {
        this.key = key;
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
