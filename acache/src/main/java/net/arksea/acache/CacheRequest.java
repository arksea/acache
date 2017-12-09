package net.arksea.acache;

import akka.routing.ConsistentHashingRouter;
import net.arksea.base.ServiceRequest;

/**
 * Created by xiaohaixing_dian91 on 2017/3/30.
 */
public abstract class CacheRequest<TKey,TData> extends ServiceRequest<TData>
                                               implements ConsistentHashingRouter.ConsistentHashable {
    public final TKey key;
    public CacheRequest(TKey key) {
        this.key = key;
    }

    @Override
    public Object consistentHashKey() {
        return key;
    }
}
