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
    public Future<DataResult<K, V>> ask(K key) {
        return ask(new GetData<>(key));
    }

    @Override
    public Future<DataResult<K, V>> ask(ICacheRequest<K, V> req) {
        return ask(req, this.timeout);
    }

    @Override
    public Future<DataResult<K, V>> ask(ICacheRequest<K, V> req, long timeout) {
        return client.request(req, timeout).mapTo(classTag((Class<DataResult<K, V>>) (Class<?>) DataResult.class));
    }

    @Override
    public Future<V> get(K key) {
        return get(new GetData<>(key));
    }

    @Override
    public Future<V> get(ICacheRequest<K, V> req) {
        return get(req, this.timeout);
    }

    @Override
    public Future<V> get(ICacheRequest<K, V> req, long timeout) {
        Future<DataResult<K,V>> f = ask(req, timeout);
        return f.map(
            new Mapper<DataResult<K,V>,V>() {
                public V apply(DataResult<K,V> ret) {
                    if (ret.failed == null) {
                        return ret.data;
                    } else {
                        throw new RuntimeException(ret.cacheName+"获取数据失败", ret.failed);
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
        DataResult<K, V> ret;
        try {
            Future<DataResult<K, V>> f = ask(key);
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
