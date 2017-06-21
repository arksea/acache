package net.arksea.acache;

import akka.actor.ActorRef;

import java.util.ArrayList;
import java.util.List;

/**
 * 根据请求类型，和缓存的值，构造返回数据
 * Created by xiaohaixing_dian91 on 2017/3/31.
 */
public interface IResponser<TData> {
    default void send(TimedData<TData> timedData,ActorRef sender){}
    default void failed(Throwable ex,ActorRef sender) {}
}
class DoNothingResponser<TData> implements IResponser<TData> {}

class GetDataResponser<TData> implements IResponser<TData> {
    ActorRef receiver;
    String cacheName;
    GetData get;
    public GetDataResponser(GetData get,ActorRef receiver,String cacheName) {
        this.get = get;
        this.receiver = receiver;
        this.cacheName = cacheName;
    }
    @Override
    public void send(TimedData<TData> timedData, ActorRef sender) {
        receiver.tell(new DataResult<>(cacheName, get.key, timedData.time, timedData.data), sender);
    }
    @Override
    public void failed(Throwable ex,ActorRef sender) {
        receiver.tell(new DataResult<>(ex, cacheName, get.key), sender);
    }
}

class GetRangeResponser<TData> implements IResponser<TData> {
    ActorRef receiver;
    String cacheName;
    GetRange get;
    public GetRangeResponser(GetRange get,ActorRef receiver,String cacheName) {
        this.get = get;
        this.receiver = receiver;
        this.cacheName = cacheName;
    }
    @Override
    public void send(TimedData<TData> timedData,ActorRef sender) {
        if (timedData.data instanceof List) {
            List array = (List) timedData.data;
            int size = array.size();
            int end = get.count > size - get.start ? size : get.start + get.count;
            if (get.start > end) {
                ArrayList list = new ArrayList<>(0);
                receiver.tell(new DataResult<>(cacheName, get.key, timedData.time,list), sender);
            } else {
                ArrayList list = new ArrayList(array.subList(get.start, end));
                receiver.tell(new DataResult<>(cacheName, get.key, timedData.time,list), sender);

            }
        } else {
            receiver.tell(new DataResult<>(cacheName, get.key, timedData.time, timedData.data), sender);
        }
    }
    @Override
    public void failed(Throwable ex,ActorRef sender) {
        receiver.tell(new DataResult<>(ex, cacheName, get.key), sender);
    }
}