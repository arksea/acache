package net.arksea.acache;

import akka.actor.ActorRef;
import scala.concurrent.Future;

import java.util.Map;

/**
 *
 * Created by arksea on 2016/11/17.
 */
public interface IDataSource<TKey, TData> extends ICacheConfig<TKey> {
    /**
     *
     * @param key
     * @return 返回timeddata=null，请求者将得到CacheResponse.code == ErrorCode.INVALID_KEY， result=null的结果
     *         返回timeddata.data=null，请求者将得到CacheResponse.code == ErrorCode.SUCCEED, result=null的结果
     */
    Future<TimedData<TData>> request(ActorRef cacheActor, String cacheName, TKey key);
    /**
     * 此处定义的是阻塞式的同步初始化，需要异步初始化可以通过GetData请求进行初始化
     * @return
     */
    default Map<TKey, TimedData<TData>> initCache(ActorRef cacheActor) {
        return null;
    }

    /**
     * 当数据被标识为脏后会回调此接口进行必要的处理
     * @param cacheActor
     * @param event
     */
    default void afterDirtyMarked(ActorRef cacheActor, MarkDirty event) {
        //default donothing
    }
}
