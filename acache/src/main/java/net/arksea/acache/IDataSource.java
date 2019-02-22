package net.arksea.acache;

import akka.actor.ActorRef;
import scala.concurrent.Future;

import java.util.List;
import java.util.Map;

/**
 *
 * Created by arksea on 2016/11/17.
 */
public interface IDataSource<TKey, TData> {
    /**
     *
     * @param key
     * @return 返回timeddata=null表示结果不缓存，返回failed给请求方
     */
    Future<TimedData<TData>> request(ActorRef cacheActor, String cacheName, TKey key);
    default void preStart(ActorRef cacheActor,String cacheName) {
        //default donothing
    }
    /**
     * 此处定义的是阻塞式的同步初始化，需要异步初始化可以不实现此接口，在外部通过调用GetData请求进行初始化
     * @param keys
     * @return
     */
    default Map<TKey, TimedData<TData>> initCache(List<TKey> keys) {
        return null;
    }
    default void afterDirtyMarked(ActorRef cacheActor, String cacheName, TKey key) {
        //default donothing
    }
}
