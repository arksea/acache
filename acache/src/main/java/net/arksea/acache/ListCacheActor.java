package net.arksea.acache;

import akka.actor.Props;
import akka.japi.Creator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * 数据本地缓存
 * Created by arksea on 2016/11/17.
 */
public class ListCacheActor<TKey> extends CacheActor<TKey,List> {
    private static final Logger log = LogManager.getLogger(ListCacheActor.class);
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
        } else {
            super.onReceive(o);
        }
    }
    //-------------------------------------------------------------------------------------
    protected void handleGetRange(final GetRange<TKey> req) {
        final String cacheName = state.config.getCacheName();
        final CachedItem<TKey,List> item = state.cacheMap.get(req.key);
        GetRangeResponser responser = new GetRangeResponser(req, sender(), cacheName);
        if (item == null) { //缓存的初始状态，新建一个CachedItem，从数据源读取数据
            log.trace("({})缓存未命中，发起更新请求，key={}", cacheName, req.key);
            requestData(req.key, responser);
        } else if (item.isExpired()) { //数据已过期
            if (item.isUpdateBackoff()) {
                log.trace("({})缓存过期，更新请求Backoff中，key={}", cacheName, req.key);
                responser.send(item.getData(), self());
            } else {
                item.onRequestUpdate(state.config.getMaxBackoff());
                if (state.config.waitForRespond()) {
                    log.trace("({})缓存过期，发起更新请求，key={}", cacheName, req.key);
                    requestData(req.key, responser);
                } else {
                    log.trace("({})缓存过期，发起更新请求，暂时使用旧数据返回请求者，key={}", cacheName, req.key);
                    responser.send(item.getData(), self());
                    requestData(req.key, doNothing);
                }
            }
        } else {//数据未过期
            log.trace("({})命中缓存，key={}", cacheName, req.key);
            responser.send(item.getData(), self());
        }
    }
}
