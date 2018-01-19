package net.arksea.acache;

import akka.actor.Props;
import akka.japi.Creator;
import akka.routing.ConsistentHashingPool;
import akka.routing.ConsistentHashingRouter;

import java.util.List;

/**
 * 数据本地缓存
 * Created by arksea on 2016/11/17.
 */
public class ListCacheActor<TKey> extends AbstractCacheActor<TKey,List> {
    public ListCacheActor(CacheActorState<TKey,List> state) {
        super(state);
    }

    public static <TKey> Props props(final IDataSource<TKey,List> dataRequest) {
        return Props.create(ListCacheActor.class, new Creator<ListCacheActor>() {
            @Override
            public ListCacheActor<TKey> create() throws Exception {
                CacheActorState<TKey,List> state = new CacheActorState<>(dataRequest);
                return new ListCacheActor<>( state);
            }
        });
    }

    public static <TKey extends ConsistentHashingRouter.ConsistentHashable>
    Props propsOfCachePool(int poolSize, IDataSource<TKey,List> cacheSource) {
        ConsistentHashingPool pool = new ConsistentHashingPool(poolSize);
        return pool.props(props(cacheSource));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void onReceive(Object o) {
        if (o instanceof GetRange) {
            handleGetRange((GetRange<TKey>) o);
        } else if (o instanceof GetSize) {
            handleGetSize((GetSize<TKey>) o);
        } else {
            super.onReceive(o);
        }
    }
    //-------------------------------------------------------------------------------------
    protected void handleGetRange(final GetRange<TKey> req) {
        final String cacheName = state.dataSource.getCacheName();
        GetRangeResponser responser = new GetRangeResponser(req, sender(), cacheName);
        handleRequest(req, responser);
    }

    protected void handleGetSize(final GetSize<TKey> req) {
        final String cacheName = state.dataSource.getCacheName();
        GetSizeResponser responser = new GetSizeResponser(req, sender(), cacheName);
        handleRequest(req, responser);
    }
}
