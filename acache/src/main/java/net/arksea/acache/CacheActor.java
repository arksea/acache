package net.arksea.acache;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.dispatch.OnFailure;
import akka.dispatch.OnSuccess;
import akka.pattern.Patterns;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static akka.japi.Util.classTag;

/**
 * 数据本地缓存
 * Created by arksea on 2016/11/17.
 */
public class CacheActor<TKey, TData> extends UntypedActor {

    private static final Logger log = LogManager.getLogger(CacheActor.class);
    private final CacheActorState<TKey,TData> state;
    private final String cacheName;

    public CacheActor(String cacheName, CacheActorState<TKey,TData> state) {
        this.cacheName = cacheName;
        this.state = state;
    }

    public static Future<DataResult> ask(GetData get, long timeout) {
        return Patterns.ask(get.cacheActor, get, timeout)
            .mapTo(classTag(DataResult.class));
    }

    @Override
    public void preStart() {
        //当IdleTimeout不为零时，启动过期缓存清除定时器
        long idleTimeout = state.config.getIdleTimeout();
        long period = Math.max(60000, idleTimeout/10); //清除周期不少于60秒钟，且不多于60分钟
        period = Math.min(3600000, period);
        if ( idleTimeout > 0) {
            context().system().scheduler().schedule(
                Duration.create(period, TimeUnit.MILLISECONDS),
                Duration.create(period, TimeUnit.MILLISECONDS),
                self(),
                new CleanTick(),
                context().dispatcher(),
                self());
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void onReceive(Object msg) {
        if (msg instanceof CleanTick) {
            handleCleanTick();
        } else if (msg instanceof GetData) {
            handleGetData((GetData<TKey>)msg);
        } else if (msg instanceof DataResult) {
            handleUpdate((DataResult<TKey,TData>)msg);
        } else if (msg instanceof Failed) {
            handleFailed((Failed<TKey>)msg);
        } else {
            unhandled(msg);
        }
    }

    private void handleGetData(final GetData<TKey> req) {
        final ActorRef requester = sender();
        final OnSuccess<TData> onSuccess = new OnSuccess<TData>() {
            @Override
            public void onSuccess(TData data) throws Throwable {
                if (data == null) {
                    Failed failed = new Failed<>(req.key,requester, new IllegalArgumentException("Cache的数据源返回Null:"+cacheName));
                    self().tell(failed, self());
                } else {
                    DataResult result = new DataResult<>(cacheName, req.key, data);
                    self().tell(result, self());
                    requester.tell(result, self());
                }
            }
        };
        final OnFailure onFailure = new OnFailure(){
            @Override
            public void onFailure(Throwable error) throws Throwable {
                Failed failed = new Failed<>(req.key,requester, error);
                self().tell(failed, self());
            }
        };
        final CachedItem<TKey,TData> item = state.cacheMap.get(req.key);
        if (item == null) { //缓存的初始状态，新建一个CachedItem，从数据源读取数据
            log.trace("({})缓存未命中，向数据源发起更新请求，key={}", cacheName, req.key);
            final Future<TData> future = state.dataRequestor.request(req.key);
            future.onSuccess(onSuccess, context().dispatcher());
            future.onFailure(onFailure, context().dispatcher());
        } else if (item.isTimeout(state.config.getLiveTimeout())) { //数据已过期
            if (item.isUpdateBackoff()) {
                log.trace("({})缓存过期，更新请求Backoff中，key={}", cacheName, req.key);
                sender().tell(new DataResult<>(this.cacheName, req.key, item.getData()), getSelf());
            } else {
                log.trace("({})缓存过期，向数据源发起更新请求，key={}", cacheName, req.key);
                item.onRequestUpdate(state.config.getMaxBackoff());
                final Future<TData> future = state.dataRequestor.request(req.key);
                future.onSuccess(onSuccess, context().dispatcher());
                future.onFailure(onFailure, context().dispatcher());
            }
        } else {//数据未过期
            log.trace("({})命中缓存，key={}，data={}", cacheName, req.key, item.getData());
            sender().tell(new DataResult<>(cacheName, req.key, item.getData()), self());
        }
    }

    private void handleUpdate(final DataResult<TKey,TData> req) {
        CachedItem<TKey,TData> item = state.cacheMap.get(req.key);
        if (item == null) {
            log.trace("({})新建缓存，key={}", cacheName, req.key);
            item = new CachedItem<>(req.key);
            state.cacheMap.put(req.key, item);
        } else {
            log.trace("({})更新缓存,key={},data={}", cacheName, req.key, req.data);
        }
        item.setData(req.data);
    }

    private void handleFailed(final Failed<TKey> failed) {
        final CachedItem item = state.cacheMap.get(failed.key);
        if (item==null) {
            log.warn("({})更新缓存失败，无可用数据，通知请求者已失败，key={}", cacheName, failed.key, failed.error);
            DataResult result = new DataResult<>(cacheName, failed.key, failed.error);
            failed.requester.tell(result, self());
        } else {
            log.warn("({})更新缓存失败，使用旧数据返回请求者，key={}, data={}", cacheName, failed.key, item.getData(), failed.error);
            DataResult result = new DataResult<>(cacheName, failed.key, item.getData());
            failed.requester.tell(result, self());
        }
    }

    /**
     * 清除过期缓存
     */
    private void handleCleanTick() {
        final List<CachedItem<TKey,TData>> expired = new LinkedList<>();
        for (CachedItem<TKey,TData> item : state.cacheMap.values()) {
            if (item.isTimeout(state.config.getIdleTimeout())) {
                expired.add(item);
            }
        }
        for (CachedItem<TKey,TData> it : expired) {
            state.cacheMap.remove(it.key);
        }
    }

    /**
     * 缓存过期检查定时通知
     */
    private final static class CleanTick {
    }
    private final static class Failed<TKey> implements Serializable{
        final TKey key;
        ActorRef requester;
        final Throwable error;
        Failed(TKey key, ActorRef requester, Throwable error) {
            this.key = key;
            this.requester = requester;
            this.error = error;
        }
    }
}
