package net.arksea.acache;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Cancellable;
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
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static akka.japi.Util.classTag;

/**
 * 数据本地缓存
 * Created by arksea on 2016/11/17.
 */
public abstract class AbstractCacheActor<TKey, TData> extends UntypedActor {
    private static final Logger log = LogManager.getLogger(AbstractCacheActor.class);
    protected final CacheActorState<TKey,TData> state;
    private static Random random = new Random(System.currentTimeMillis());
    protected final DoNothingResponser<TKey> doNothing = new DoNothingResponser<>();
    private Cancellable cleanTickTimer;
    private Cancellable updateTickTimer;
    public AbstractCacheActor(CacheActorState<TKey,TData> state) {
        this.state = state;
    }

    @SuppressWarnings("unchecked")
    public static <K,V> Future<DataResult<K,V>> ask(ActorSelection cacheActor, ICacheRequest req, long timeout) {
        return Patterns.ask(cacheActor, req, timeout)
            .mapTo(classTag((Class<DataResult<K, V>>) (Class<?>) DataResult.class));
    }

    @Override
    public void preStart() {
        initCache();
        //当idleCleanPeriod不为零时，启动过期缓存清除定时器
        long idleCleanPeriod = state.config.getIdleCleanPeriod();
        if ( idleCleanPeriod > 0) {
            //清除周期不少于60秒钟，且不多于60分钟
            long period = Math.max(60000, idleCleanPeriod);
            period = Math.min(3600000, period) + random.nextInt(60000); //加随机数为了分散不同实例清除时间，调试日志也比较容易分辨
            cleanTickTimer = context().system().scheduler().schedule(
                Duration.create(period, TimeUnit.MILLISECONDS),
                Duration.create(period, TimeUnit.MILLISECONDS),
                self(),
                new CleanTick(),
                context().dispatcher(),
                self());
        }
        //当autoUpdatePeriod不为零时，启动过期缓存自动更新定时器
        long autoUpdatePeriod = state.config.getAutoUpdatePeriod();
        if (autoUpdatePeriod > 0) {
            //自动更新周期不少于60秒钟，且不多于60分钟
            long period = Math.max(60000, autoUpdatePeriod);
            period = Math.min(3600000, period) + random.nextInt(60000); //加随机数为了分散不同实例清除时间，调试日志也比较容易分辨
            updateTickTimer = context().system().scheduler().schedule(
                Duration.create(period, TimeUnit.MILLISECONDS),
                Duration.create(period, TimeUnit.MILLISECONDS),
                self(),
                new UpdateTick(),
                context().dispatcher(),
                self());
        }
        state.dataSource.preStart(self(),state.config.getCacheName());
        log.debug("Start CacheActor {}", self().path().toStringWithoutAddress());
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();
        log.debug("CacheActor {} stopped", self().path().toStringWithoutAddress());
        if (cleanTickTimer != null) {
            cleanTickTimer.cancel();
            cleanTickTimer = null;
        }
        if (updateTickTimer != null) {
            updateTickTimer.cancel();
            updateTickTimer = null;
        }
    }

    private void initCache() {
        List<TKey> keys = state.config.getInitKeys();
        if (keys != null && !keys.isEmpty()) {
            log.info("初始化缓存({})", state.config.getCacheName());
            Map<TKey, TimedData<TData>> items = state.dataSource.initCache(keys);
            if (items != null) {
                for (Map.Entry<TKey, TimedData<TData>> e : items.entrySet()) {
                    CachedItem<TKey, TData> item = new CachedItem<>(e.getKey());
                    TimedData<TData> value = e.getValue();
                    item.setData(value.data, value.time);
                    state.cacheMap.put(e.getKey(), item);
                }
                log.info("初始化缓存({})完成，共加载{}项", state.config.getCacheName(), items.size());
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void onReceive(Object o) {
        if (o instanceof GetData) {
            handleGetData((GetData<TKey,TData>)o);
        } else if (o instanceof DataResult) {
            handleDataResult((DataResult<TKey, TData>) o);
        } else if (o instanceof MarkDirty) {
            handleMarkDirty((MarkDirty)o);
        } else if (o instanceof Failed) {
            handleFailed((Failed<TKey>) o);
        } else if (o instanceof GetHitStat) {
            handleGetHitStat();
        } else if (o instanceof GetAndResetHitStat) {
            handleGetAndResetHitStat();
        } else if (o instanceof CleanTick) {
            handleCleanTick();
        } else if (o instanceof UpdateTick) {
            handleUpdateTick();
        } else {
            unhandled(o);
        }
    }

    //-------------------------------------------------------------------------------------
    private void handleGetData(final GetData<TKey,TData> req) {
        final String cacheName = state.config.getCacheName();
        GetDataResponser responser = new GetDataResponser(req, sender(), cacheName);
        handleRequest(req, responser);
    }

    protected void handleRequest(final ICacheRequest<TKey,TData> req, IResponser responser) {
        state.requestCount++;
        TKey key = req.getKey();
        final String cacheName = state.config.getCacheName();
        final CachedItem<TKey,TData> item = state.cacheMap.get(key);
        if (item == null) { //缓存的初始状态，新建一个CachedItem，从数据源读取数据
            state.respondMiss++;
            log.trace("({})缓存未命中，发起更新请求，key={}", cacheName, key);
            requestData(key, responser);
        } else if (item.isExpired()) { //数据已过期
            state.respondMiss++;
            if (item.isUpdateBackoff()) {
                log.trace("({})缓存过期，更新请求Backoff中，key={}", cacheName, key);
                responser.send(item.getData(), self());
            } else {
                item.onRequestUpdate(state.config.getMaxBackoff());
                if (state.config.waitForRespond()) {
                    log.trace("({})缓存过期，发起更新请求，key={}", cacheName, key);
                    requestData(key, responser);
                } else {
                    log.trace("({})缓存过期，发起更新请求，暂时使用旧数据返回请求者，key={}", cacheName, key);
                    responser.send(item.getData(), self());
                    requestData(key, doNothing);
                }
            }
        } else {//数据未过期
            state.respondMiss++;
            log.trace("({})命中缓存，key={}", cacheName, key);
            responser.send(item.getData(), self());
        }
    }

    protected void requestData(TKey key,IResponser responser) {
        try {
            final Future<TimedData<TData>> future = state.dataSource.request(self(),state.config.getCacheName(), key);
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
            log.trace("({})更新缓存,key={}", cacheName, req.key);
        }
        item.setData(req.data,req.expiredTime);
    }
    //-------------------------------------------------------------------------------------
    protected void handleFailed(final Failed<TKey> failed) {
        final String cacheName = state.config.getCacheName();
        final CachedItem item = state.cacheMap.get(failed.key);
        if (item==null) {
            if (failed.responser == doNothing) {
                log.warn("({})请求新数据失败，key={}", cacheName, failed.key, failed.error);
            } else {
                log.warn("({})请求新数据失败；无可用数据，通知请求者已失败，key={}", cacheName, failed.key, failed.error);
            }
            failed.responser.failed(failed.error, self());
        } else {
            if (failed.responser == doNothing) {
                log.warn("({})请求新数据失败，key={}", cacheName, failed.key, failed.error);
            } else {
                log.warn("({})请求新数据失败；使用旧数据返回请求者，key={}", cacheName, failed.key, failed.error);
            }
            failed.responser.send(item.getData(), self());
        }
    }
    //-------------------------------------------------------------------------------------
    protected void handleMarkDirty(MarkDirty<TKey,TData> event) {
        final String cacheName = state.config.getCacheName();
        final CachedItem<TKey,TData> item = state.cacheMap.get(event.key);
        if (item == null) {
            log.debug("({})尝试标记缓存为脏数据，但缓存未命中，key={}", cacheName, event.key);
        } else {
            log.debug("({})标记缓存为脏数据，key={}", cacheName, event.key);
            item.markDirty();
        }
        state.dataSource.afterDirtyMarked(self(), cacheName, event);
    }
    //-------------------------------------------------------------------------------------
    protected void onSuccessData(final TKey key, final Future<TimedData<TData>> future, IResponser responser) {
        final String cacheName = state.config.getCacheName();
        ActorRef cacheActor = self();
        final OnSuccess<TimedData<TData>> onSuccess = new OnSuccess<TimedData<TData>>() {
            @Override
            public void onSuccess(TimedData<TData> timedData) throws Throwable {
                if (timedData == null) {
                    Exception ex = new IllegalArgumentException(cacheName+"."+key+"没有对应的数据");
                    responser.failed(ex, ActorRef.noSender());
                } else {
                    cacheActor.tell(new DataResult<>(cacheName, key, timedData.time, timedData.data), ActorRef.noSender());
                    responser.send(timedData, ActorRef.noSender());
                }
            }
        };
        future.onSuccess(onSuccess, context().dispatcher());
    }

    protected void onFailureData(final TKey key, final Future<TimedData<TData>> future, IResponser responser) {
        ActorRef cacheActor = self();
        final OnFailure onFailure = new OnFailure(){
            @Override
            public void onFailure(Throwable error) throws Throwable {
                Failed failed = new Failed<>(key,responser, error);
                cacheActor.tell(failed, ActorRef.noSender());
            }
        };
        future.onFailure(onFailure, context().dispatcher());
    }
    //-------------------------------------------------------------------------------------
    /**
     * 清除Idle过期缓存
     */
    protected void handleCleanTick() {
        final List<CachedItem<TKey,TData>> expired = new LinkedList<>();
        long now = System.currentTimeMillis();
        log.trace("{} cacheMap.size = {}",state.config.getCacheName(),state.cacheMap.size());
        for (CachedItem<TKey,TData> item : state.cacheMap.values()) {
            long idleTimeout = state.config.getIdleTimeout(item.key);
            log.trace("{} idleTimeout={}",state.config.getCacheName(),idleTimeout);
            if (idleTimeout > 0) {
                //数据闲置的过期时间
                long idleExpiredTime = item.getLastRequestTime() + idleTimeout;
                log.trace("{} idleExpiredTime={}  idleExpiredTime-now={}",state.config.getCacheName(),idleExpiredTime,idleExpiredTime-now);
                if (now > idleExpiredTime) {
                    expired.add(item);
                    continue;
                }
            }
            //数据被标识为过期移除则删除缓存, 注意：不能调用item.getData().removeOnExpired获取状态
            //因为getData()会修改lastRequestTime，让item的idle无法过期
            if (item.isExpired() && item.isRemoveOnExpired()) {
                expired.add(item);
            }
        }
        log.trace("{} expired.size = {}",state.config.getCacheName(),expired.size());
        if (expired.size() > 0) {
            log.debug("'{}' has items = {}, to be cleaned items = {}",
                state.config.getCacheName(), state.cacheMap.size(), expired.size());
        }
        for (CachedItem<TKey,TData> it : expired) {
            state.cacheMap.remove(it.key);
        }
        expired.clear();
    }

    /**
     * 自动更新过期数据
     */
    protected void handleUpdateTick() {
        log.trace("{} cacheMap.size = {}",state.config.getCacheName(),state.cacheMap.size());
        for (CachedItem<TKey,TData> item : state.cacheMap.values()) {
            boolean isAutoUpdate = state.config.isAutoUpdateExpiredData(item.key);
            if (isAutoUpdate && item.isExpired()) {
                log.debug("{} auto update {}",state.config.getCacheName(),item.key);
                requestData(item.key, doNothing);
            }
        }
    }

    /**
     * 缓存过期检查定时通知
     */
    final static class CleanTick {
    }
    final static class UpdateTick {
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

    //----------------------------------------------------------------------------------------------------------
    private void handleGetHitStat() {
        HitStat msg = new HitStat(state.requestCount, state.respondHit,state.respondMiss,state.respondExpired);
        sender().tell(msg, self());
    }
    //----------------------------------------------------------------------------------------------------------
    private void handleGetAndResetHitStat() {
        HitStat msg = new HitStat(state.requestCount, state.respondHit,state.respondMiss,state.respondExpired);
        state.requestCount = 0;
        state.respondHit = 0;
        state.respondExpired = 0;
        state.respondMiss = 0;
        sender().tell(msg, self());
    }

}
