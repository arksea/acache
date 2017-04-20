package net.arksea.acache;

import akka.actor.ActorSelection;
import akka.dispatch.Mapper;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;

/**
 *
 * Created by arksea on 2016/11/17.
 */
public final class CacheAsker<K,V> {

    public final long timeout;
    public final ActorSelection cacheActor;
    public final ExecutionContext dispatcher;

    public CacheAsker(ActorSelection cacheActor, ExecutionContext dispatcher, long timeout) {
        this.timeout = timeout;
        this.cacheActor = cacheActor;
        this.dispatcher = dispatcher;
    }

    public Future<DataResult<K,V>> ask(ICacheRequest<K,V> req) {
        Future<DataResult<K,V>> f = CacheActor.ask(cacheActor, req, timeout);
        return f.map(
            new Mapper<DataResult<K,V>,DataResult<K,V>>() {
                public DataResult<K,V> apply(DataResult<K,V> ret) {
                    if (ret.failed == null) {
                        if (ret.data == null) {
                            throw new RuntimeException(ret.cacheName+"返回的数据为null");
                        } else {
                            return ret;
                        }
                    } else {
                        throw new RuntimeException(ret.cacheName+"获取数据失败", ret.failed);
                    }
                }
            }, dispatcher);
    }
}
