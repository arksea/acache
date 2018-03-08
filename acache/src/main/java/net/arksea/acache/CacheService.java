package net.arksea.acache;

import akka.actor.ActorRef;
import akka.dispatch.Mapper;
import scala.concurrent.Await;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

/**
 * 简化访问Cache写法的帮助类，
 * get系列用于简单的直接获取值，
 * ask系列用于多个不同请求需要在返回值处理时区分返回结果地方
 * syncGet是同步访问，仅建议用于测试或者特殊的情境
 * Created by arksea on 2016/11/17.
 */
public final class CacheService<K,V> {
    public final long timeout;
    public final ActorRef cacheActor;
    public final ExecutionContext dispatcher;

    public CacheService(ActorRef cacheActor, ExecutionContext dispatcher, long timeout) {
        this.timeout = timeout;
        this.cacheActor = cacheActor;
        this.dispatcher = dispatcher;
    }

    public void markDirty(K key) {
        cacheActor.tell(new MarkDirty<>(key), ActorRef.noSender());
    }

    /**
     * 直接用key作为参数
     * @param key
     * @return
     */
    public Future<CacheResponse<K,V>> ask(K key) {
        return ask(new GetData<>(key));
    }

    public Future<CacheResponse<K,V>> ask(CacheRequest<K,V> req) {
        return ask(req, this.timeout);
    }

    public Future<CacheResponse<K,V>> ask(CacheRequest<K,V> req, long timeout) {
        return CacheActor.ask(cacheActor, req, timeout);
    }

    public Future<V> get(K key) {
        return get(new GetData<>(key));
    }

    public Future<V> get(CacheRequest<K,V> req) {
        return get(req, this.timeout);
    }

    public Future<V> get(CacheRequest<K,V> req, long timeout) {
        Future<CacheResponse<K,V>> f = CacheActor.ask(cacheActor, req, timeout);
        return f.map(
            new Mapper<CacheResponse<K,V>,V>() {
                public V apply(CacheResponse<K,V> ret) {
                    if (ret.code == ErrorCodes.SUCCEED) {
                        return ret.result;
                    } else {
                        throw new CacheAskException(ret.toString());
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
    public V syncGet(K key) throws CacheAskException {
        CacheResponse<K, V> ret;
        try {
            Future<CacheResponse<K, V>> f = CacheActor.ask(cacheActor, new GetData(key), timeout);
            Duration d = Duration.create(timeout, "ms");
            ret = Await.result(f, d);
        } catch (Exception ex) {
            throw new CacheAskException("get cache failed", ex);
        }
        if (ret.code != ErrorCodes.SUCCEED) {
            throw new CacheAskException(ret.toString());
        }
        return ret.result;
    }
}
