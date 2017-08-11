package net.arksea.acache;

import akka.actor.ActorSelection;
import akka.dispatch.Mapper;
import scala.concurrent.Await;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

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

    public Future<DataResult<K,V>> ask(K key) {
        return ask(new GetData(key));
    }

    public Future<DataResult<K,V>> ask(ICacheRequest<K,V> req) {
        return ask(req, this.timeout);
    }

    public Future<DataResult<K,V>> ask(ICacheRequest<K,V> req, long timeout) {
        Future<DataResult<K,V>> f = CacheActor.ask(cacheActor, req, timeout);
        return f.map(
            new Mapper<DataResult<K,V>,DataResult<K,V>>() {
                public DataResult<K,V> apply(DataResult<K,V> ret) {
                    if (ret.failed == null) {
                        return ret;
                    } else {
                        throw new RuntimeException(ret.cacheName+"获取数据失败", ret.failed);
                    }
                }
            }, dispatcher);
    }

    /**
     * 同步访问方法不应作为常规使用手段，建议用于测试或者少数特殊场景
     * @param key
     * @return
     * @throws Exception
     */
    public V get(K key) throws CacheAskException {
        DataResult<K, V> ret;
        try {
            Future<DataResult<K, V>> f = CacheActor.ask(cacheActor, new GetData(key), timeout);
            Duration d = Duration.create(timeout, "ms");
            ret = Await.result(f, d);
        } catch (Exception ex) {
            throw new CacheAskException("get cache failed", ex);
        }
        if (ret!=null && ret.failed != null) {
            throw new CacheAskException("get cache failed", ret.failed);
        }
        return ret.data;
    }
}
