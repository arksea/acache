package net.arksea.acache;

import akka.actor.ActorRef;
import net.arksea.dsf.service.ServiceRequest;
import net.arksea.dsf.service.ServiceResponse;

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
    ServiceRequest request;
    public GetDataResponser(GetData get, ActorRef receiver, String cacheName, ServiceRequest request) {
        this.get = get;
        this.receiver = receiver;
        this.cacheName = cacheName;
        this.request = request;
    }
    @Override
    public void send(TimedData<TData> timedData, ActorRef sender) {
        Object result = new CacheResponse<>(ErrorCodes.SUCCEED, "ok", get.reqid, get.key, timedData.data, cacheName, timedData.time);
        Object msg = request == null ?  result : new ServiceResponse(result, request);
        receiver.tell(msg, sender);

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
    ServiceRequest request;
    public GetRangeResponser(GetRange get,ActorRef receiver,String cacheName,ServiceRequest request) {
        this.get = get;
        this.receiver = receiver;
        this.cacheName = cacheName;
        this.request = request;
    }
    @Override
    public void send(TimedData<List> timedData,ActorRef sender) {
        int size = timedData.data.size();
        int end = get.count > size - get.start ? size : get.start + get.count;
        ArrayList list = get.start >= end ? new ArrayList<>(0)
            : new ArrayList(timedData.data.subList(get.start, end));
        Object result = new CacheResponse<>(ErrorCodes.SUCCEED, "ok", get.reqid, get.key, list, cacheName, timedData.time);
        Object msg = request == null ?  result : new ServiceResponse(result, request);
        receiver.tell(msg, sender);
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
    ServiceRequest request;
    public GetSizeResponser(GetSize get,ActorRef receiver,String cacheName,ServiceRequest request) {
        this.get = get;
        this.receiver = receiver;
        this.cacheName = cacheName;
        this.request = request;
    }
    @Override
    public void send(TimedData<List> timedData, ActorRef sender) {
        Integer result =  timedData.data.size();
        Object msg = request == null ?  result : new ServiceResponse(result, request);
        receiver.tell(msg, sender);
    }
    @Override
    public void failed(int code, String error,ActorRef sender) {
        Integer result = -1;
        Object msg = request == null ?  result : new ServiceResponse(result, request);
        receiver.tell(msg, sender);
    }
}