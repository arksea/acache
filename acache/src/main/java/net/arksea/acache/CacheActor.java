package net.arksea.acache;

import akka.actor.*;
import akka.dispatch.OnFailure;
import akka.dispatch.OnSuccess;
import akka.japi.Creator;
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
    protected final CacheActorState<TKey,TData> state;
    private final DoNothingResponser<TKey> doNothing = new DoNothingResponser<>();
    public CacheActor(CacheActorState<TKey,TData> state) {
        this.state = state;
    }

    //Actor重启时继承原Actor状态与缓存
    public static <TKey,TData> Props props(final ICacheConfig config, final IDataSource<TKey,TData> dataRequest) {
        return Props.create(CacheActor.class, new Creator<CacheActor>() {
            CacheActorState<TKey,TData> state = new CacheActorState<>(config,dataRequest);
            @Override
            public CacheActor<TKey, TData> create() throws Exception {
                return new CacheActor<>( state);
            }
        });
    }

    public static <K,V> Future<DataResult<K,V>> ask(ActorSelection cacheActor, ICacheRequest req, long timeout) {
        return Patterns.ask(cacheActor, req, timeout)
            .mapTo(classTag((Class<DataResult<K, V>>) (Class<?>) DataResult.class));
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
    public void onReceive(Object o) {
        if (o instanceof GetData) {
            handleGetData((GetData<TKey,TData>)o);
        } else if (o instanceof GetRange) {
            handleGetRange((GetRange<TKey,TData>) o);
        } else if (o instanceof DataResult) {
            handleDataResult((DataResult<TKey, TData>) o);
        } else if (o instanceof ModifyData) {
            handleModifyData((ModifyData)o);
        } else if (o instanceof Failed) {
            handleFailed((Failed<TKey>) o);
        } else if (o instanceof CleanTick) {
            handleCleanTick();
        } else {
            handleOthers(o);
        }
    }

    @SuppressWarnings("unchecked")
    protected void handleOthers(Object o) {
        unhandled(o);
    }
    //-------------------------------------------------------------------------------------
    private void handleGetData(final GetData<TKey,TData> req) {
        final String cacheName = state.config.getCacheName();
        final CachedItem<TKey,TData> item = state.cacheMap.get(req.key);
        GetDataResponser responser = new GetDataResponser(req, sender(), cacheName);
        if (item == null) { //缓存的初始状态，新建一个CachedItem，从数据源读取数据
            log.trace("({})缓存未命中，发起更新请求，key={}", cacheName, req.key);
            requestData(req.key, responser);
        } else if (item.isTimeout(state.config.getLiveTimeout(item.key))) { //数据已过期
            if (item.isUpdateBackoff()) {
                log.trace("({})缓存过期，更新请求Backoff中，key={}", cacheName, req.key);
                responser.send(item.getData(), false, self());
            } else {
                log.trace("({})缓存过期，发起更新请求，key={}", cacheName, req.key);
                item.onRequestUpdate(state.config.getMaxBackoff());
                if (state.config.waitForRespond()) {
                    requestData(req.key, responser);
                } else {
                    requestData(req.key, doNothing);
                    responser.send(item.getData(),false,self());
                }
            }
        } else {//数据未过期
            log.trace("({})命中缓存，key={}，data={}", cacheName, req.key, item.getData());
            responser.send(item.getData(), true, self());
        }
    }

    protected void requestData(TKey key,IResponser responser) {
        try {
            final Future<TData> future = state.dataSource.request(key);
            onSuccessData(key, future, responser);
            onFailureData(key, future, responser);
        } catch (Exception ex) {
            handleFailed(new Failed<>(key,responser,ex));
        }
    }
    //-------------------------------------------------------------------------------------
    protected void handleDataResult(final DataResult<TKey,TData> req) {
        final String cacheName = state.config.getCacheName();
        CachedItem<TKey,TData> item = state.cacheMap.get(req.key);
        if (item == null) {
            log.trace("({})新建缓存，key={}", cacheName, req.key);
            item = new CachedItem<>(req.key);
            state.cacheMap.put(req.key, item);
        } else {
            log.trace("({})更新缓存,key={},data={}", cacheName, req.key, req.data);
        }
        item.setData(req.data, req.newest);
    }
    //-------------------------------------------------------------------------------------
    protected void handleFailed(final Failed<TKey> failed) {
        final String cacheName = state.config.getCacheName();
        final CachedItem item = state.cacheMap.get(failed.key);
        if (item==null) {
            log.warn("({})请求新数据失败；无可用数据，通知请求者已失败，key={}", cacheName, failed.key, failed.error);
            failed.responser.failed(failed.error, self());
        } else {
            log.warn("({})请求新数据失败；使用旧数据返回请求者，key={}", cacheName, failed.key, failed.error);
            failed.responser.send(item.getData(), false, self());
        }
    }
    //-------------------------------------------------------------------------------------
    protected void handleModifyData(ModifyData<TKey,TData> req) {
        final String cacheName = state.config.getCacheName();
        final Future<TData> future = state.dataSource.modify(req.key, req.modifier);
        ModifyDataResponser responser = new ModifyDataResponser(req, sender(), cacheName);
        onSuccessData(req.key, future, responser);
        onFailureData(req.key, future, responser);
    }
    //-------------------------------------------------------------------------------------
    protected void onSuccessData(final TKey key, final Future<TData> future, IResponser responser) {
        final String cacheName = state.config.getCacheName();
        final OnSuccess<TData> onSuccess = new OnSuccess<TData>() {
            @Override
            public void onSuccess(TData data) throws Throwable {
                if (data == null) {
                    Exception ex = new IllegalArgumentException("("+cacheName+") CacheActor的数据源返回Null");
                    Failed failed = new Failed<>(key,responser, ex);
                    self().tell(failed, self());
                } else {
                    self().tell(new DataResult<>(cacheName, key, data), self());
                    responser.send(data, true, self());
                }
            }
        };
        future.onSuccess(onSuccess, context().dispatcher());
    }

    protected void onFailureData(final TKey key, final Future<TData> future, IResponser responser) {
        final OnFailure onFailure = new OnFailure(){
            @Override
            public void onFailure(Throwable error) throws Throwable {
                Failed failed = new Failed<>(key,responser, error);
                self().tell(failed, self());
            }
        };
        future.onFailure(onFailure, context().dispatcher());
    }
    //-------------------------------------------------------------------------------------
    protected void handleGetRange(final GetRange<TKey,TData> req) {
        final String cacheName = state.config.getCacheName();
        final CachedItem<TKey,TData> item = state.cacheMap.get(req.key);
        GetRangeResponser responser = new GetRangeResponser(req, sender(), cacheName);
        if (item == null) { //缓存的初始状态，新建一个CachedItem，从数据源读取数据
            log.trace("({})缓存未命中，发起更新请求，key={}", cacheName, req.key);
            requestData(req.key, responser);
        } else if (item.isTimeout(state.config.getLiveTimeout(item.key))) { //数据已过期
            if (item.isUpdateBackoff()) {
                log.trace("({})缓存过期，更新请求Backoff中，key={}", cacheName, req.key);
                responser.send(item.getData(), false, self());
            } else {
                log.trace("({})缓存过期，发起更新请求，key={}", cacheName, req.key);
                item.onRequestUpdate(state.config.getMaxBackoff());
                if (state.config.waitForRespond()) {
                    requestData(req.key, responser);
                } else {
                    requestData(req.key, doNothing);
                    responser.send(item.getData(), false, self());
                }
            }
        } else {//数据未过期
            log.trace("({})命中缓存，key={}，data={}", cacheName, req.key, item.getData());
            responser.send(item.getData(), true, self());
        }
    }

    //-------------------------------------------------------------------------------------
    /**
     * 清除过期缓存
     */
    protected void handleCleanTick() {
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
    final static class CleanTick {
    }
    final static class Failed<TKey> implements Serializable{
        final TKey key;
        IResponser responser;
        final Throwable error;
        Failed(TKey key, IResponser responser, Throwable error) {
            this.key = key;
            this.responser = responser;
            this.error = error;
        }
    }
}
