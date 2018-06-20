package net.arksea.acache;

import akka.routing.ConsistentHashingRouter;

import java.util.List;

/**
 *
 * Created by xiaohaixing_dian91 on 2017/3/30.
 */
public class GetRange<TKey> implements ICacheRequest<TKey,List> {
    public final TKey key;
    public final int start;
    public final int count;

    public GetRange(TKey key,int start,int count) {
        this.key = key;
        this.start = start;
        this.count = count;
    }

    public TKey getKey() {
        return key;
    }

    @Override
    public int hashCode() {
        return key.hashCode() + start*31 + count*31;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof GetRange) {
            GetRange g = (GetRange) o;
            return start==g.start && count==g.count && this.key.equals(g.key);
        } else {
            return false;
        }
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
