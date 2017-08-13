package net.arksea.acache;

import akka.actor.Props;
import akka.japi.Creator;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * 数据本地缓存
 * Created by arksea on 2016/11/17.
 */
public class ListCacheActor<TKey> extends CacheActor<TKey,List> {
//    private static final Logger log = LogManager.getLogger(ListCacheActor.class);
    public ListCacheActor(CacheActorState<TKey,List> state) {
        super(state);
    }

    public static <TKey> Props listCacheProps(final ICacheConfig config, final IDataSource<TKey,List> dataRequest) {
        return Props.create(ListCacheActor.class, new Creator<ListCacheActor>() {
            CacheActorState<TKey,List> state = new CacheActorState<>(config,dataRequest);
            @Override
            public ListCacheActor<TKey> create() throws Exception {
                return new ListCacheActor<>( state);
            }
        });
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
        final String cacheName = state.config.getCacheName();
        GetRangeResponser responser = new GetRangeResponser(req, sender(), cacheName);
        handleRequest(req, responser);
    }

    protected void handleGetSize(final GetSize<TKey> req) {
        final String cacheName = state.config.getCacheName();
        GetSizeResponser responser = new GetSizeResponser(req, sender(), cacheName);
        handleRequest(req, responser);
    }
}
