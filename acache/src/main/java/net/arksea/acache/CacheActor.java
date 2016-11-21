package net.arksea.acache;

import akka.actor.*;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
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
    private final CacheActorState<TKey,TData> state;
    private boolean selfIsLeader = true;
    private ActorSelection leader;
    private final static int ASK_TIMEOUT = 10000;
    private final Cluster cluster = Cluster.get(getContext().system());

    private CacheActor(CacheActorState<TKey,TData> state) {
        this.state = state;
    }

    //Actor重启时继承原Actor状态与缓存
    public static <TKey,TData> Props heritableProps(final ICacheConfig config, final IDataSource<TKey,TData> dataRequest) {
        return Props.create(CacheActor.class, new Creator<CacheActor>() {
            CacheActorState<TKey,TData> state = new CacheActorState<>(config,dataRequest);
            @Override
            public CacheActor<TKey, TData> create() throws Exception {
                return new CacheActor<>( state);
            }
        });
    }

    //Actor重启时新建缓存
    public static <TKey,TData> Props props(final ICacheConfig config, final IDataSource<TKey,TData> dataRequest) {
        return Props.create(CacheActor.class, new Creator<CacheActor>() {
            @Override
            public CacheActor<TKey, TData> create() throws Exception {
                return new CacheActor<>(new CacheActorState<>(config,dataRequest));
            }
        });
    }

    public static Future<DataResult> ask(GetData get, long timeout) {
        return Patterns.ask(get.cacheActor, get, timeout)
            .mapTo(classTag(DataResult.class));
    }

    @Override
    public void preStart() {
        cluster.subscribe(self(), ClusterEvent.ClusterDomainEvent.class);
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
        if (o instanceof CleanTick) {
            handleCleanTick();
        } else if (o instanceof GetData) {
            handleGetData((GetData<TKey>)o);
        } else if (o instanceof DataResult) {
            handleUpdate((DataResult<TKey,TData>)o);
        } else if (o instanceof Failed) {
            handleFailed((Failed<TKey>)o);
        } else if (o instanceof ClusterEvent.LeaderChanged) {
            ClusterEvent.LeaderChanged msg = (ClusterEvent.LeaderChanged) o;
            checkLeader(msg.getLeader());
        } else if (o instanceof ClusterEvent.RoleLeaderChanged) {
            ClusterEvent.RoleLeaderChanged msg = (ClusterEvent.RoleLeaderChanged) o;
            checkLeader(msg.getLeader());
        } else if (o instanceof ClusterEvent.CurrentClusterState) {
            ClusterEvent.CurrentClusterState msg = (ClusterEvent.CurrentClusterState)o;
            checkLeader(msg.getLeader());
        } else {
            unhandled(o);
        }
    }

    private void handleGetData(final GetData<TKey> req) {
        final String cacheName = self().path().name();
        final CachedItem<TKey,TData> item = state.cacheMap.get(req.key);
        if (item == null) { //缓存的初始状态，新建一个CachedItem，从数据源读取数据
            log.trace("({})缓存未命中，发起更新请求，key={}, selfIsLeader={}", cacheName, req.key, selfIsLeader);
            requestData(req);
        } else if (item.isTimeout(state.config.getLiveTimeout())) { //数据已过期
            if (item.isUpdateBackoff()) {
                log.trace("({})缓存过期，更新请求Backoff中，key={}", cacheName, req.key);
                sender().tell(new DataResult<>(cacheName, req.key, item.getData()), getSelf());
            } else {
                log.trace("({})缓存过期，发起更新请求，key={}, selfIsLeader={}", cacheName, req.key, selfIsLeader);
                item.onRequestUpdate(state.config.getMaxBackoff());
                requestData(req);
            }
        } else {//数据未过期
            log.trace("({})命中缓存，key={}，data={}", cacheName, req.key, item.getData());
            sender().tell(new DataResult<>(cacheName, req.key, item.getData()), self());
        }
    }

    private void requestData(final GetData<TKey> req) {
        try {
            if (selfIsLeader || !state.config.updateFromMaster()) {
                requestFromSource(req);
            } else {
                requestFromMaster(req);
            }
        } catch (Exception ex) {
            handleFailed(new Failed<>(req.key,sender(),ex));
        }
    }

    private void requestFromSource(final GetData<TKey> req) {
        final String cacheName = self().path().name();
        final ActorRef requester = sender();
        final OnSuccess<TData> onSuccess = new OnSuccess<TData>() {
            @Override
            public void onSuccess(TData data) throws Throwable {
                if (data == null) {
                    Failed failed = new Failed<>(req.key,requester, new IllegalArgumentException("("+cacheName+") CacheActor的数据源返回Null"));
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
        final Future<TData> future = state.dataSource.request(req.key);
        future.onSuccess(onSuccess, context().dispatcher());
        future.onFailure(onFailure, context().dispatcher());
    }

    private void requestFromMaster(final GetData<TKey> req) {
        final ActorRef requester = sender();
        final OnSuccess<DataResult<TKey, TData>> onSuccess = new OnSuccess<DataResult<TKey, TData>>() {
            @Override
            public void onSuccess(DataResult<TKey, TData> result) throws Throwable {
                if (result.failed == null) {
                    self().tell(result, self());
                    requester.tell(result, self());
                } else {
                    self().tell(result.failed, self());
                }
            }
        };
        final OnFailure onFailure = new OnFailure(){
            @Override
            public void onFailure(Throwable error) throws Throwable {
                Failed failed = new Failed<>(req.key, requester, error);
                self().tell(failed, self());
            }
        };
        Future<DataResult<TKey, TData>> future = Patterns.ask(leader, new GetData<TKey>(leader.anchor(), req.key), ASK_TIMEOUT)
            .mapTo(classTag((Class<DataResult<TKey, TData>>) (Class<?>) DataResult.class));
        future.onSuccess(onSuccess, context().dispatcher());
        future.onFailure(onFailure, context().dispatcher());
    }

    private void handleUpdate(final DataResult<TKey,TData> req) {
        final String cacheName = self().path().name();
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
        final String cacheName = self().path().name();
        final CachedItem item = state.cacheMap.get(failed.key);
        if (item==null) {
            log.warn("({})更新缓存失败；无可用数据，通知请求者已失败，key={}", cacheName, failed.key, failed.error);
            DataResult result = new DataResult<>(cacheName, failed.key, failed.error);
            failed.requester.tell(result, self());
        } else {
            log.warn("({})更新缓存失败；使用旧数据返回请求者，key={}, data={}", cacheName, failed.key, item.getData(), failed.error);
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

    private void checkLeader(Address leaderAddr) {
        leader = context().actorSelection(self().path().toStringWithAddress(leaderAddr));
        selfIsLeader = cluster.selfAddress().equals(leaderAddr);
        log.info("({}) CacheActor Leader's Address: {}", self().path().name(), leaderAddr);
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
