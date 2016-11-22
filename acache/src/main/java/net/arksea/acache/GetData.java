package net.arksea.acache;

import akka.actor.ActorRef;
import akka.routing.ConsistentHashingRouter;

/**
 *
 * Created by arksea on 2016/11/17.
 */
public class GetData<TKey> implements ConsistentHashingRouter.ConsistentHashable  {
    public final TKey key;

    public GetData(TKey key) {
        this.key = key;
    }

    @Override
    public Object consistentHashKey() {
        return key;
    }
}
