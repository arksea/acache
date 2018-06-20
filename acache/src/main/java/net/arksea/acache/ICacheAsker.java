package net.arksea.acache;

import scala.concurrent.Future;

/**
 *
 * Created by xiaohaixing on 2018/5/4.
 */
public interface ICacheAsker<K,V> {
    /**
     * 直接用key作为参数
     * @param key
     * @return
     */
    Future<DataResult<K,V>> ask(K key);

    Future<DataResult<K,V>> ask(ICacheRequest<K,V> req);

    Future<DataResult<K,V>> ask(ICacheRequest<K,V> req, long timeout);

    Future<V> get(K key);

    Future<V> get(ICacheRequest<K,V> req);

    Future<V> get(ICacheRequest<K,V> req, long timeout);

    Future<Integer> getSize(K key);
    /**
     * 同步访问方法不应作为常规使用手段，建议用于测试或者少数特殊场景
     * @param key
     * @return
     * @throws CacheAskException
     */
    V syncGet(K key) throws CacheAskException;
}
