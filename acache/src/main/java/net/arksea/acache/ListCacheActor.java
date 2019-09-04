package net.arksea.acache;

import akka.actor.Props;
import akka.japi.Creator;
import akka.routing.ConsistentHashingPool;
import akka.routing.ConsistentHashingRouter;
import net.arksea.dsf.service.ServiceRequest;

import java.util.List;

/**
 * 数据本地缓存
 * Created by arksea on 2016/11/17.
 */
public class ListCacheActor<TKey> extends AbstractCacheActor<TKey,List> {
    public ListCacheActor(CacheActorState<TKey,List> state) {
        super(state);
    }

    public static <TKey> Props props(final ICacheConfig config, final IDataSource<TKey,List> dataRequest) {
        return props(config, dataRequest, new IHitStat<TKey>() {});
    }

    public static <TKey> Props props(final ICacheConfig config, final IDataSource<TKey,List> dataRequest, final IHitStat<TKey> hitStat) {
        return Props.create(ListCacheActor.class, new Creator<ListCacheActor>() {
            @Override
            public ListCacheActor<TKey> create() throws Exception {
                CacheActorState<TKey,List> state = new CacheActorState<>(config,dataRequest,hitStat);
                return new ListCacheActor<>( state);
            }
        });
    }

    public static <TKey extends ConsistentHashingRouter.ConsistentHashable>
    Props propsOfCachePool(int poolSize, ICacheConfig<TKey> cacheConfig, IDataSource<TKey,List> cacheSource) {
        return propsOfCachePool(poolSize, cacheConfig, cacheSource, new IHitStat<TKey>() {});
    }

    public static <TKey extends ConsistentHashingRouter.ConsistentHashable>
    Props propsOfCachePool(int poolSize, ICacheConfig<TKey> cacheConfig, IDataSource<TKey,List> cacheSource, final IHitStat<TKey> hitStat) {
        ConsistentHashingPool pool = new ConsistentHashingPool(poolSize);
        return pool.props(props(cacheConfig, cacheSource, hitStat));
    }

    protected void onReceiveCacheMsg(Object o, ServiceRequest serviceRequest) {
        if (o instanceof GetRange) {
            handleGetRange((GetRange<TKey>) o, serviceRequest);
        } else if (o instanceof GetSize) {
            handleGetSize((GetSize<TKey>) o, serviceRequest);
        } else {
            super.onReceiveCacheMsg(o, serviceRequest);
        }
    }

    //-------------------------------------------------------------------------------------
    protected void handleGetRange(final GetRange<TKey> req, ServiceRequest serviceRequest) {
        final String cacheName = state.config.getCacheName();
        GetRangeResponser responser = new GetRangeResponser(req, sender(), cacheName, serviceRequest);
        handleRequest(req, responser);
    }

    protected void handleGetSize(final GetSize<TKey> req, ServiceRequest serviceRequest) {
        final String cacheName = state.config.getCacheName();
        GetSizeResponser responser = new GetSizeResponser(req, sender(), cacheName, serviceRequest);
        handleRequest(req, responser);
    }
}
