package net.arksea.acache;

import akka.actor.ActorRef;
import akka.routing.ConsistentHashingRouter;

/**
 *
 * Created by arksea on 2016/11/17.
 */
public class GetData<TKey> implements ConsistentHashingRouter.ConsistentHashable  {
    public final ActorRef cacheActor;
    public final TKey key;

    public GetData(ActorRef cacheActor, TKey key) {
        this.cacheActor = cacheActor;
        this.key = key;
    }

    @Override
    public Object consistentHashKey() {
        return key;
    }
}
