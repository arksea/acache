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
    default void failed(int code, String error,ActorRef sender) {}
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
        receiver.tell(new CacheResponse<>(ErrorCodes.SUCCEED, "ok", get.reqid, get.key, timedData.data, cacheName, timedData.time), sender);
    }
    @Override
    public void failed(int code, String error,ActorRef sender) {
        receiver.tell(new CacheResponse<>(code, error, get.reqid, get.key, cacheName), sender);
    }
}

class GetRangeResponser implements IResponser<List> {
    ActorRef receiver;
    String cacheName;
    GetRange get;
    public GetRangeResponser(GetRange get,ActorRef receiver,String cacheName) {
        this.get = get;
        this.receiver = receiver;
        this.cacheName = cacheName;
    }
    @Override
    public void send(TimedData<List> timedData,ActorRef sender) {
        int size = timedData.data.size();
        int end = get.count > size - get.start ? size : get.start + get.count;
        if (get.start >= end) {
            ArrayList list = new ArrayList<>(0);
            receiver.tell(new CacheResponse<>(ErrorCodes.SUCCEED, "ok", get.reqid, get.key, list, cacheName, timedData.time), sender);
        } else {
            ArrayList list = new ArrayList(timedData.data.subList(get.start, end));
            receiver.tell(new CacheResponse<>(ErrorCodes.SUCCEED, "ok", get.reqid, get.key, list, cacheName, timedData.time), sender);
        }
    }
    @Override
    public void failed(int code,String error,ActorRef sender) {
        receiver.tell(new CacheResponse<>(code, error, get.reqid, get.key, cacheName), sender);
    }
}

class GetSizeResponser implements IResponser<List> {
    ActorRef receiver;
    String cacheName;
    GetSize get;
    public GetSizeResponser(GetSize get,ActorRef receiver,String cacheName) {
        this.get = get;
        this.receiver = receiver;
        this.cacheName = cacheName;
    }
    @Override
    public void send(TimedData<List> timedData, ActorRef sender) {
        receiver.tell(timedData.data.size(), sender);
    }
    @Override
    public void failed(int code, String error,ActorRef sender) {
        receiver.tell(-1, sender);
    }
}