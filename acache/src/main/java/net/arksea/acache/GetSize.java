package net.arksea.acache;

import akka.routing.ConsistentHashingRouter;

import java.util.List;

/**
 *
 * Created by xiaohaixing on 2017/8/13.
 */
public class GetSize<TKey> implements ICacheRequest<TKey,List> {
    public final TKey key;

    public GetSize(TKey key) {
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
