package net.arksea.acache;

import akka.actor.ActorRef;
import scala.concurrent.Future;

/**
 *
 * Created by arksea on 2016/11/17.
 */
public interface IDataSource<TKey, TData> {
    Future<TimedData<TData>> request(TKey key);
    default void preStart(ActorRef cacheActor,String cacheName) {
        //default donothing
    }
    default void afterDirtyMarked(ActorRef cacheActor, String cacheName, EventDirty event) {
        //default donothing
    }
}
