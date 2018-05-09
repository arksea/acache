package net.arksea.acache.dsf;

import akka.actor.ActorRef;
import akka.dispatch.Mapper;
import net.arksea.acache.*;
import net.arksea.dsf.client.Client;
import scala.concurrent.Await;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import static akka.japi.Util.classTag;

/**
 *
 * Created by xiaohaixing on 2018/5/8.
 */
public final class CacheDsfAsker<K,V> implements ICacheAsker<K,V> {
    public final long timeout;
    private Client client;
    public final ExecutionContext dispatcher;

    public CacheDsfAsker(Client cacheClient, ExecutionContext dispatcher, long timeout) {
        this.timeout = timeout;
        this.client = cacheClient;
        this.dispatcher = dispatcher;
    }

    public void markDirty(K key) {
        client.tell(new MarkDirty<>(key), true, ActorRef.noSender());
    }

    @Override
    public Future<CacheResponse<K, V>> ask(K key) {
        return ask(new GetData<>(key));
    }

    @Override
    public Future<CacheResponse<K, V>> ask(CacheRequest<K, V> req) {
        return ask(req, this.timeout);
    }

    @Override
    public Future<CacheResponse<K, V>> ask(CacheRequest<K, V> req, long timeout) {
        return client.request(req, timeout).mapTo(classTag((Class<CacheResponse<K, V>>) (Class<?>) CacheResponse.class));
    }

    @Override
    public Future<V> get(K key) {
        return get(new GetData<>(key));
    }

    @Override
    public Future<V> get(CacheRequest<K, V> req) {
        return get(req, this.timeout);
    }

    @Override
    public Future<V> get(CacheRequest<K, V> req, long timeout) {
        Future<CacheResponse<K,V>> f = ask(req, timeout);
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

    @Override
    public Future<Integer> getSize(K key) {
        GetSize<K> getSize = new GetSize<>(key);
        return client.request(getSize, timeout).mapTo(classTag(Integer.class));
    }

    /**
     * 同步访问方法不应作为常规使用手段，建议用于测试或者少数特殊场景
     * @param key
     * @return
     * @throws Exception
     */
    @Override
    public V syncGet(K key) throws CacheAskException {
        CacheResponse<K, V> ret;
        try {
            Future<CacheResponse<K, V>> f = ask(key);
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
