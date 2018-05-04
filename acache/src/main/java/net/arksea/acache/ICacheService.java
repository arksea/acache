package net.arksea.acache;

import akka.pattern.Patterns;
import scala.concurrent.Future;

import static akka.japi.Util.classTag;

/**
 *
 * Created by xiaohaixing on 2018/5/4.
 */
public interface ICacheService<K,V> {

    /**
     * 直接用key作为参数
     * @param key
     * @return
     */
    public Future<CacheResponse<K,V>> ask(K key);

    public Future<CacheResponse<K,V>> ask(CacheRequest<K,V> req);

    public Future<CacheResponse<K,V>> ask(CacheRequest<K,V> req, long timeout);

    public Future<V> get(K key);

    public Future<V> get(CacheRequest<K,V> req);

    public Future<V> get(CacheRequest<K,V> req, long timeout);

    public Future<Integer> getSize(K key);
    /**
     * 同步访问方法不应作为常规使用手段，建议用于测试或者少数特殊场景
     * @param key
     * @return
     * @throws Exception
     */
    public V syncGet(K key) throws CacheAskException;
}
