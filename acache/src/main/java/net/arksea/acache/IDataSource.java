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
    Future<TimedData<TData>> request(TKey key);
    default void preStart(ActorRef cacheActor,String cacheName) {
        //default donothing
    /**
     * 此处定义的是阻塞式的同步初始化，需要异步初始化可以不实现此接口，在外部通过调用GetData请求进行初始化
     * @param keys
     * @return
     */
    default Map<TKey, TimedData<TData>> initCache(List<TKey> keys) {
        return null;
    }
    default Future<TimedData<TData>> modify(TKey key, IDataModifier<TData> modifier) {
        throw new UnsupportedOperationException();
    }
    default void afterDirtyMarked(ActorRef cacheActor, String cacheName, EventDirty event) {
        //default donothing
    }
}
