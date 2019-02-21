package net.arksea.acache;

import akka.actor.Props;
import akka.japi.Creator;
import akka.routing.ConsistentHashingPool;
import akka.routing.ConsistentHashingRouter;

/**
 * 数据本地缓存
 * Created by arksea on 2016/11/17.
 */
public class CacheActor<TKey, TData> extends AbstractCacheActor {

    public CacheActor(CacheActorState<TKey,TData> state) {
        super(state);
    }

    public static <TKey,TData> Props props(final ICacheConfig config, final IDataSource<TKey,TData> dataRequest) {
        return props(config, dataRequest, new IHitStat<TKey>() {});
    }

    //Actor重启时继承原Actor状态与缓存
    public static <TKey,TData> Props props(final ICacheConfig config, final IDataSource<TKey,TData> dataRequest, final IHitStat<TKey> hitStat) {
        return Props.create(CacheActor.class, new Creator<CacheActor>() {
            @Override
            public CacheActor<TKey, TData> create() throws Exception {
                //此state必须放在每次创建实例时，否则会引起建立pooled cache时使用同一个state
                CacheActorState<TKey,TData> state = new CacheActorState<>(config,dataRequest,hitStat);
                return new CacheActor<>( state);
            }
        });
    }

    public static <TKey extends ConsistentHashingRouter.ConsistentHashable,TData>
    Props propsOfCachePool(int poolSize, ICacheConfig<TKey> cacheConfig, IDataSource<TKey,TData> cacheSource) {
        return propsOfCachePool(poolSize, cacheConfig, cacheSource, new IHitStat<TKey>() {});
    }

    public static <TKey extends ConsistentHashingRouter.ConsistentHashable,TData>
    Props propsOfCachePool(int poolSize, ICacheConfig<TKey> cacheConfig, IDataSource<TKey,TData> cacheSource, final IHitStat<TKey> hitStat) {
        ConsistentHashingPool pool = new ConsistentHashingPool(poolSize);
        return pool.props(props(cacheConfig, cacheSource, hitStat));
    }
}
