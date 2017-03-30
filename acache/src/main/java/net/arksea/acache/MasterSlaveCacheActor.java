package net.arksea.acache;

import akka.actor.*;
import akka.cluster.Cluster;
import akka.cluster.Member;
import akka.dispatch.OnFailure;
import akka.dispatch.OnSuccess;
import akka.japi.Creator;
import akka.pattern.Patterns;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.concurrent.Future;

import static akka.japi.Util.classTag;

/**
 * Master Slave结构的数据本地缓存集群
 * Created by arksea on 2016/11/17.
 */
public class MasterSlaveCacheActor<TKey, TData> extends CacheActor<TKey, TData> {

    protected static final Logger log = LogManager.getLogger(MasterSlaveCacheActor.class);
    private final Cluster cluster = Cluster.get(getContext().system());

    public MasterSlaveCacheActor(CacheActorState<TKey,TData> state) {
        super(state);
    }

    //Actor重启时继承原Actor状态与缓存
    public static <TKey,TData> Props props(final ICacheConfig config, final IDataSource<TKey,TData> dataRequest) {
        return Props.create(MasterSlaveCacheActor.class, new Creator<MasterSlaveCacheActor>() {
            CacheActorState<TKey,TData> state = new CacheActorState<>(config,dataRequest);
            @Override
            public MasterSlaveCacheActor<TKey, TData> create() throws Exception {
                return new MasterSlaveCacheActor<>( state);
            }
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void handleOthers(Object o) {
        unhandled(o);
    }
    //-------------------------------------------------------------------------------------
    @Override
    protected void requestData(final GetData<TKey> req) {
        try {
            if (selfIsLeader() || !state.config.updateByMaster()) {
                final Future<TData> future = state.dataSource.request(req.key);
                onSuccessData(req.key, future);
                onFailureData(req.key, future);
            } else {
                Future<DataResult<TKey, TData>> future = Patterns.ask(getLeader(), req, ASK_TIMEOUT)
                    .mapTo(classTag((Class<DataResult<TKey, TData>>) (Class<?>) DataResult.class));
                onSuccessResult(future);
                onFailureResult(req.key, future);
            }
        } catch (Exception ex) {
            handleFailed(new Failed<>(req.key,sender(),ex));
        }
    }
    @Override
    protected void requestRange(final GetRange<TKey> req) {
        try {
            if (selfIsLeader() || !state.config.updateByMaster()) {
                final Future<TData> future = state.dataSource.request(req.key);
                onSuccessData(req, future);
                onFailureData(req.key, future);
            } else {
                Future<DataResult<TKey, TData>> future = Patterns.ask(getLeader(), req, ASK_TIMEOUT)
                    .mapTo(classTag((Class<DataResult<TKey, TData>>) (Class<?>) DataResult.class));
                onSuccessResult(future);
                onFailureResult(req.key, future);
            }
        } catch (Exception ex) {
            handleFailed(new Failed<>(req.key,sender(),ex));
        }
    }
    //-------------------------------------------------------------------------------------
    @Override
    protected void handleDataResult(final DataResult<TKey,TData> req) {
        super.handleDataResult(req);
        //同步数据到集群，注意：发送同步数据时重新new了一个sync参数为false的DataResult
        //此为冗余安全保护措施，防止多个节点都认为自己是Master时系统因自激震荡而崩溃
        if (req.sync && selfIsLeader() && state.config.autoSync()) {
            DataResult<TKey,TData> data = new DataResult<>(req.cacheName, req.key, req.data, req.newest, false);
            for (Member m : cluster.state().getMembers()) {
                if (!cluster.selfAddress().equals(m.address())) {
                    ActorSelection s = context().actorSelection(self().path().toStringWithAddress(m.address()));
                    s.tell(data, self());
                }
            }
        }
    }
    //-------------------------------------------------------------------------------------
    @Override
    protected void handleModifyData(ModifyData<TKey,TData> req) {
        final String cacheName = self().path().name();
        try {
            if (selfIsLeader() || !state.config.updateByMaster()) {
                final Future<TData> future = state.dataSource.modify(req.key, req.modifier);
                onSuccessData(req.key, future);
                onFailureData(req.key, future);
            } else {
                Future<DataResult<TKey, TData>> future = Patterns.ask(getLeader(), req, ASK_TIMEOUT)
                    .mapTo(classTag((Class<DataResult<TKey, TData>>) (Class<?>) DataResult.class));
                onSuccessResult(future);
                onFailureResult(req.key, future);
            }
        } catch (Exception ex) {
            log.warn("({})修改缓存失败；key={}", cacheName, req.key, ex);
        }
    }
    //-------------------------------------------------------------------------------------
    protected void onSuccessResult(final Future<DataResult<TKey, TData>> future) {
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
        future.onSuccess(onSuccess, context().dispatcher());
    }

    protected void onFailureResult(final TKey key, final Future<DataResult<TKey, TData>> future) {
        final ActorRef requester = sender();
        final OnFailure onFailure = new OnFailure(){
            @Override
            public void onFailure(Throwable error) throws Throwable {
                Failed failed = new Failed<>(key, requester, error);
                self().tell(failed, self());
            }
        };
        future.onFailure(onFailure, context().dispatcher());
    }

    private ActorSelection getLeader() {
        Address address = cluster.state().getLeader();
        return context().actorSelection(self().path().toStringWithAddress(address));
    }

    private boolean selfIsLeader() {
        return cluster.selfAddress().equals(cluster.state().getLeader());
    }
}
